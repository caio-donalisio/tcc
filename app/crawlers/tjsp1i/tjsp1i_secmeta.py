import logging
import random
import time
import re
from app.crawlers import base, browsers, utils
import requests
import click
import time
import pendulum
from app.crawler_cli import cli
from app.celery_run import celery_app as celery
from app.crawlers.tjsp1i.tjsp1i_utils import list_pending_secmetas
from app.crawlers.logconfig import logger_factory
import concurrent.futures
import urllib
from bs4 import BeautifulSoup


from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By

logger = logger_factory('tjsp1i-secmeta')

#Secmeta = Secondary metadata
class TJSP1IDownloader:

  def __init__(self, output):
    # self._client = client
    self._output = output
    # self.browser = browsers.FirefoxBrowser(headless=False)

  @utils.retryable(max_retries=5, sleeptime=2, retryable_exceptions=Exception, message='Connection Error')
  def download(self, items, pbar=None):

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
      futures  = []
      last_run = None
      for item in items:
          # response = self._get_response(item)

        # secmeta_content = utils.get_response(logger, requests.Session(), item.url, '').text
        response = self._get_response(item)
        if pbar:
          pbar.update(1)

        # up async
        if response.content:
          futures.append(executor.submit(self._handle_upload, item, response))

      for future in concurrent.futures.as_completed(futures):
        future.result()

  def _handle_upload(self, item, secmeta_content):
    logger.debug(f'GET {item} UPLOAD')

    if secmeta_content and len(secmeta_content.text) > 100:
      self._output.save_from_contents(
          filepath=f"{item.dest}_SEC.html",
          contents=secmeta_content.text,
          content_type='text/html')
    else:
      logger.warn(f"Got empty document for {item.dest}.html")

  @utils.retryable(max_retries=3)
  def _get_response(self, content_from_url):
    response=utils.get_response(logger, requests.Session(), content_from_url.src, '')
    logger.debug(f'GET {content_from_url.src}')
    # for cookie in self._client.request_cookies_browser:
    #   self._client.session.cookies.set(cookie['name'], cookie['value'])

    if self._output.exists(content_from_url.dest):
      return None

    # response = utils.get_response()
    if 'text/html' in response.headers.get('Content-type'):
      # logger.info(f'Code {response.status_code} (OK) for URL {content_from_url.src}.')
      return response
    else:
      logger.info(f'Code {response.status_code} for URL {content_from_url.src}.')
      logger.warn(
        f"Got {response.status_code} when fetching {content_from_url.src}. Content-type: {response.headers.get('Content-type')}.")
      raise utils.PleaseRetryException()

@celery.task(name='tjsp1i.secmeta', autoretry_for=(Exception,),
             default_retry_delay=60, max_retries=3)
def tjsp1i_download_task(items, output_uri):
  from tqdm import tqdm

  time.sleep(random.uniform(5., 15.))

  output = utils.get_output_strategy_by_path(path=output_uri)
  downloader = TJSP1IDownloader(output)

  tqdm_out = utils.TqdmToLogger(logger, level=logging.INFO)


  with tqdm(total=len(items), file=tqdm_out) as pbar:
    to_download = []

    for item in items:
      to_download.append(
        base.ContentFromURL(
          src=item['url'],
          dest=item['dest'])
        )
      downloader.download(to_download, pbar)


def tjsp1i_download(items, output_uri, pbar):
  output     = utils.get_output_strategy_by_path(path=output_uri)
  downloader = TJSP1IDownloader(output=output)
  to_download = []
  for _, item in enumerate(items):
    to_download.append(
        base.ContentFromURL(
          src=item['url'],
          dest=item['dest'],
          content_type='text/html')
        )
  downloader.download(to_download, pbar)

@cli.command(name='tjsp1i-secmeta')
@click.option('--start-date',
  default=utils.DefaultDates.THREE_MONTHS_BACK.strftime("%Y-%m"),
  help='Format YYYY-MM.',
)
@click.option('--end-date'  ,
  default=utils.DefaultDates.NOW.strftime("%Y-%m"),
  help='Format YYYY-MM.',
)
@click.option('--input-uri'   , help='Input URI')
@click.option('--dry-run'     , default=False, is_flag=True)
@click.option('--local'       , default=False, is_flag=True)
@click.option('--count'       , default=False, is_flag=True)
@click.option('--max-workers' , default=7)
@click.option('--batch' , default=100)
def tjsp1i_secmeta_command(input_uri, start_date, end_date, dry_run, local, count, max_workers, batch):
  # batch  = []
  global MAX_WORKERS
  MAX_WORKERS=int(max_workers)
  output = utils.get_output_strategy_by_path(path=input_uri)
  startDate = pendulum.parse(start_date)
  endDate = pendulum.parse(end_date)

  if count:
    total = 0
    while startDate <= endDate:
      for _ in list_pending_secmetas(output._bucket_name, startDate.format('YYYY/MM')):
        total += 1
      startDate = startDate.add(months=1)
    print('Total files to download', total)
    return

  while startDate <= endDate:
    print(f"TJSP1I - Collecting secondary metadata {startDate.format('YYYY/MM')}...")
    pendings = []
    counter = 0 
    for pending in list_pending_secmetas(output._bucket_name, startDate.format('YYYY/MM')):
      pendings.append(pending)
    utils.run_pending_tasks(tjsp1i_download, pendings, input_uri=input_uri, dry_run=dry_run, max_workers=max_workers)
    startDate = startDate.add(months=1)
