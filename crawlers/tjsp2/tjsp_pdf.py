import click
import pathlib
import time
import random

import pendulum
from app import cli, celery

import logging
import utils
import base
from crawlers.tjsp2.tjsp_crawler import TJSPClient, TJSPHandler
from logconfig import logger_factory, setup_cloud_logger

logger = logger_factory('tjsp-pdf')

class TJSPDownloader:

  def __init__(self, client, output):
    self._client = client
    self._output = output

  def download(self, items, pbar=None):
    import time
    import concurrent.futures
    self._client.signin()
    self._client.set_search(
      start_date=pendulum.DateTime(2020, 1, 1),
      end_date=pendulum.DateTime(2020, 1, 31),
    )
    self._client.close()

    interval = 5
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
      futures  = []
      last_run = None
      for item in items:
        # request sync
        now = time.time()
        if last_run is not None:
          since = now - last_run
          if since < interval:
            jitter     = random.uniform(.5, 1.2)
            sleep_time = (interval - since) + jitter
            time.sleep(sleep_time)

        last_run = time.time()
        response = self._get_response(item)
        if pbar:
          pbar.update(1)

        # up async
        futures.append(executor.submit(self._handle_upload, item, response))

      for future in concurrent.futures.as_completed(futures):
        future.result()

  @utils.retryable(max_retries=3)
  def _get_response(self, content_from_url):
    logger.debug(f'GET {content_from_url.src}')
    for cookie in self._client.request_cookies_browser:
      self._client.session.cookies.set(cookie['name'], cookie['value'])
    response = self._client.session.get(content_from_url.src,
      headers=self._client.header_generator.generate(),
      allow_redirects=True,
      verify=False,
      timeout=10)
    if response.headers.get('Content-type') == 'application/pdf;charset=UTF-8':
      return response
    else:
      logger.warn(
        f"Got {response.status_code} when fetching {content_from_url.src}. Content-type: {response.headers.get('Content-type')}.")
      raise utils.PleaseRetryException()

  def _handle_upload(self, content_from_url, response):
    logger.debug(f'GET {content_from_url.src} UPLOAD')
    self._output.save_from_contents(
        filepath=content_from_url.dest,
        contents=response.content,
        content_type=content_from_url.content_type)


@celery.task(queue='tjsp.pdf')
def tjsp_download_task(items, output_uri):
  from tqdm import tqdm

  time.sleep(random.uniform(5., 15.))

  output     = utils.get_output_strategy_by_path(path=output_uri)
  client     = TJSPClient()
  downloader = TJSPDownloader(client=client, output=output)

  tqdm_out = utils.TqdmToLogger(logger, level=logging.INFO)

  with tqdm(total=len(items), file=tqdm_out) as pbar:
    downloader.download(
      [
        base.ContentFromURL(
          src=item['url'],
          dest=item['dest'],
          content_type='application/pdf'
        )
        for item in items
      ],
      pbar
    )


def tjsp_download(items, output_uri, pbar):
  output     = utils.get_output_strategy_by_path(path=output_uri)
  client     = TJSPClient()
  downloader = TJSPDownloader(client=client, output=output)
  downloader.download(
    [
      base.ContentFromURL(
        src=item['url'],
        dest=item['dest'],
        content_type='application/pdf'
      )
      for item in items
    ],
    pbar
  )


def list_all(bucket_name, prefix):
  bucket = utils.get_bucket_ref(bucket_name)
  for blob in bucket.list_blobs(prefix=prefix):
    yield blob.name


def list_pending_pdfs(bucket_name, prefix):
  jsons = {}
  pdfs  = {}

  for name in list_all(bucket_name, prefix):
    path = pathlib.Path(name)
    if name.endswith(".json"):
      jsons[path.stem] = path.parent
    if name.endswith(".pdf"):
      pdfs[path.stem] = path.parent

  for name, parent in jsons.items():
    if name not in pdfs:
      cdacordao = name.split('_')[-1]
      yield {'url':
        f'http://esaj.tjsp.jus.br/cjsg/getArquivo.do?conversationId=&cdAcordao={cdacordao}&cdForo=0',
         'dest': f'{parent}/{name}.pdf'
      }


@cli.command(name='tjsp-pdf')
@click.option('--prefix')
@click.option('--input-uri'   , help='Input URI')
@click.option('--dry-run'     , default=False, is_flag=True)
@click.option('--local'       , default=False, is_flag=True)
@click.option('--count'       , default=False, is_flag=True)
def tjsp_pdf_command(input_uri, prefix, dry_run, local, count):
  batch  = []
  output = utils.get_output_strategy_by_path(path=input_uri)

  if count:
    total = 0
    for _ in list_pending_pdfs(output._bucket_name, prefix):
      total += 1
    print('Total files to download', total)
    return

  # for testing purposes
  if local:
    from tqdm import tqdm
    import concurrent.futures

    # just to count
    pendings = []
    for pending in list_pending_pdfs(output._bucket_name, prefix):
      pendings.append(pending)

    with tqdm(total=len(pendings)) as pbar:
      executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)
      futures  = []
      for pending in pendings:
        if not dry_run:
          batch.append(pending)
          if len(batch) >= 100:
            futures.append(executor.submit(tjsp_download, batch, input_uri, pbar))
            time.sleep(random.uniform(5., 8.))
            batch = []

    print("Tasks distributed -- waiting for results")
    for future in concurrent.futures.as_completed(futures):
      future.result()
    executor.shutdown()
    if len(batch):
      tjsp_download(batch, input_uri, pbar)

  else:
    total = 0
    for pending in list_pending_pdfs(output._bucket_name, prefix):
      total += 1
      batch.append(pending)
      if len(batch) >= 100:
        print("Task", tjsp_download_task.delay(batch, input_uri))
        batch = []
    if len(batch):
      print("Task", tjsp_download_task.delay(batch, input_uri))
    print('Total files to download', total)