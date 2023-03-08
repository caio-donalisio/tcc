import logging
import random
import time
import re

from app.crawlers import base, browsers, utils
import click
import pendulum
from app.crawler_cli import cli
from app.celery_run import celery_app as celery
from app.crawlers.tjsc.tjsc_crawler import TJSCClient, TJSCChunk, TJSCContentHandler

from app.crawlers.tjsc.tjsc_utils import list_invalid_files
from app.crawlers.logconfig import logger_factory

logger = logger_factory('tjsc-recollect')

class TJSCDownloader:

  def __init__(self, client=None, output=None):
    self._client = client
    self._output = output

  @utils.retryable(retryable_exceptions=Exception, ignore_if_exceeds=True)
  def download(self, items, pbar=None):
    import concurrent.futures

    handler = TJSCContentHandler(self._output)
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
      futures = []
      try:
        for item in items:
          item_soup = utils.soup_by_content(item.content)
          responses = TJSCChunk(*[None]*5).get_act_files(item_soup)
          for response in responses:
            if isinstance(response, base.ContentFromURL):
              futures.append(executor.submit(handler._handle_url_event, response, recollect=True))
          if pbar:
            pbar.update(1)
      
      except Exception as e:
        raise Exception(e)
      
      for future in concurrent.futures.as_completed(futures):
        future.result()

@celery.task(name='crawlers.tjsc.recollect', autoretry_for=(Exception,),
             default_retry_delay=60, max_retries=6)
def tjsc_download_task(items, output_uri):
  from tqdm import tqdm

  time.sleep(random.uniform(5., 15.))

  output = utils.get_output_strategy_by_path(path=output_uri)
  client = TJSCClient()
  downloader = TJSCDownloader(client=client, output=output)

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

def tjsc_download(items, output_uri, pbar):
  output = utils.get_output_strategy_by_path(path=output_uri)
  client = TJSCClient()
  downloader = TJSCDownloader(client=client, output=output)
  downloader.download(
      [
          base.Content(
              content=item['row'],
              dest=item['dest'],
              content_type='text/html'
          )
          for item in items
      ],
      pbar
  )


@cli.command(name='tjsc-recollect')
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
def tjsc_recollect_command(input_uri, start_date, end_date,max_workers, dry_run, count, batch):
  output = utils.get_output_strategy_by_path(path=input_uri)
  startDate = pendulum.parse(start_date)
  endDate = pendulum.parse(end_date)
  global MAX_WORKERS
  MAX_WORKERS = int(max_workers)

  if count:
    total = 0
    while startDate <= endDate:
      for _ in list_invalid_files(output._bucket_name, startDate.format('YYYY/MM')):
        total += 1
      startDate = startDate.add(months=1)
    print('Total files to download', total)
    return

  while startDate <= endDate:
    print(f"TJSC - Recollecting invalid files {startDate.format('YYYY/MM')}...")
    pendings = []
    counter = 0
    for pending in list_invalid_files(output._bucket_name, startDate.format('YYYY/MM')):
      pendings.append(pending)
      counter += 1
      if counter % batch == 0:
        utils.run_pending_tasks(tjsc_download, pendings, input_uri=input_uri, dry_run=dry_run)
        pendings.clear()
      startDate = startDate.add(months=1)
    utils.run_pending_tasks(tjsc_download, pendings, input_uri=input_uri, dry_run=dry_run)
    startDate = startDate.add(months=1)
