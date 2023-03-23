import logging
import random
import time

from app.crawlers import base, utils
import click
import pendulum
from app.crawler_cli import cli
from app.celery_run import celery_app as celery
from app.crawlers.tjsp.tjsp_crawler import TJSPClient
from app.crawlers.tjsp.tjsp_utils import list_pending_pdfs
from app.crawlers.logconfig import logger_factory

logger = logger_factory('tjsp-pdf')


class TJSPDownloader:

  def __init__(self, client, output):
    self._client = client
    self._output = output

  def download(self, items, pbar=None):
    import concurrent.futures
    import time
    self._client.signin()
    self._client.set_search(
        start_date=pendulum.DateTime(2020, 1, 1),
        end_date=pendulum.DateTime(2020, 1, 31),
    )
    self._client.close()

    interval = 5
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
      futures = []
      last_run = None
      for item in items:
        # request sync
        now = time.time()
        if last_run is not None:
          since = now - last_run
          if since < interval:
            jitter = random.uniform(.5, 1.2)
            sleep_time = (interval - since) + jitter
            time.sleep(sleep_time)

        response = self._get_response(item)
        if pbar:
          pbar.update(1)

        # up async
        if response:
          last_run = time.time()
          futures.append(executor.submit(self._handle_upload, item, response))

      for future in concurrent.futures.as_completed(futures):
        future.result()

  @utils.retryable(max_retries=5)
  def _get_response(self, content_from_url):
    logger.debug(f'GET {content_from_url.src}')
    for cookie in self._client.request_cookies_browser:
      self._client.session.cookies.set(cookie['name'], cookie['value'])

    if self._output.exists(content_from_url.dest):
      return None

    response = self._client.session.get(content_from_url.src,
                                        headers=self._client.header_generator.generate(),
                                        allow_redirects=True,
                                        verify=False,
                                        timeout=10)
    if not response.headers.get('Content-type'):
      raise utils.PleaseRetryException("Couldn't download file, retrying...")
    if 'application/pdf' in response.headers.get('Content-type'):
      logger.info(f'Code {response.status_code} (OK) for URL {content_from_url.src}.')
      return response
    elif 'text/html' in response.headers.get('Content-type') and \
            'Não foi possível exibir a decisão solicitada.' in response.text:
      logger.warn(f'PDF for {content_from_url.src} not available.')
    else:
      logger.info(f'Code {response.status_code} for URL {content_from_url.src}.')
      logger.warn(
          f"Got {response.status_code} when fetching {content_from_url.src}. Content-type: {response.headers.get('Content-type')}.")
      raise utils.PleaseRetryException()

  def _handle_upload(self, content_from_url, response):
    logger.debug(f'GET {content_from_url.src} UPLOAD')

    if len(response.content) > 0:
      self._output.save_from_contents(
          filepath=content_from_url.dest,
          contents=response.content,
          content_type=content_from_url.content_type)
    else:
      logger.warn(
          f"Got 0 bytes for {content_from_url.src}. Content-type: {response.headers.get('Content-type')}.")


@celery.task(name='crawlers.tjsp.pdf', autoretry_for=(Exception,),
             default_retry_delay=60, max_retries=3)
def tjsp_download_task(items, output_uri):
  from tqdm import tqdm

  time.sleep(random.uniform(5., 15.))

  output = utils.get_output_strategy_by_path(path=output_uri)
  client = TJSPClient()
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
  output = utils.get_output_strategy_by_path(path=output_uri)
  client = TJSPClient()
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


@cli.command(name='tjsp-pdf')
@click.option('--start-date',
              default=utils.DefaultDates.THREE_MONTHS_BACK.strftime("%Y-%m"),
              help='Format YYYY-MM.',
              )
@click.option('--end-date',
              default=utils.DefaultDates.NOW.strftime("%Y-%m"),
              help='Format YYYY-MM.',
              )
@click.option('--input-uri', help='Input URI')
@click.option('--max-workers', default=3, help='Number of parallel workers')
@click.option('--dry-run', default=False, is_flag=True)
@click.option('--count', default=False, is_flag=True)
@click.option('--batch', default=100)
def tjsp_pdf_command(input_uri, start_date, end_date,max_workers, dry_run, count, batch):
  output = utils.get_output_strategy_by_path(path=input_uri)
  startDate = pendulum.parse(start_date)
  endDate = pendulum.parse(end_date)
  global MAX_WORKERS
  MAX_WORKERS = int(max_workers)

  if count:
    total = 0
    while startDate <= endDate:
      for _ in list_pending_pdfs(output._bucket_name, startDate.format('YYYY/MM')):
        total += 1
      startDate = startDate.add(months=1)
    print('Total files to download', total)
    return

  while startDate <= endDate:
    print(f"TJSP - Collecting {startDate.format('YYYY/MM')}...")
    pendings = []
    counter = 0
    for pending in list_pending_pdfs(output._bucket_name, startDate.format('YYYY/MM')):
      pendings.append(pending)
      counter += 1
      if counter % batch == 0:
        utils.run_pending_tasks(tjsp_download, pendings, input_uri=input_uri, dry_run=dry_run)
        pendings.clear()
      startDate = startDate.add(months=1)
    utils.run_pending_tasks(tjsp_download, pendings, input_uri=input_uri, dry_run=dry_run)
    startDate = startDate.add(months=1)