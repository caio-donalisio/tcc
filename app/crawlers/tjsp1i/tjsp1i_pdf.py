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
from app.crawlers.tjsp1i.tjsp1i_utils import list_pending_pdfs
from app.crawlers.logconfig import logger_factory
import concurrent.futures
import urllib
from bs4 import BeautifulSoup


from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By

logger = logger_factory('tjsp1i-pdf')

class TJSP1IDownloader:

  def __init__(self, output):
    # self._client = client
    self._output = output
    # self.browser = browsers.FirefoxBrowser(headless=False)

  @utils.retryable(max_retries=5, sleeptime=1.1, retryable_exceptions=Exception, message='Browser error')
  def download(self, items, pbar=None):

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
      futures  = []
      last_run = None
      try:
        with browsers.FirefoxBrowser(headless=True) as browser:
          for item in items:
            pdf_content = self.download_files(browser, BeautifulSoup(item.content,'html.parser'))

            if pbar:
              pbar.update(1)

            # up async
            if pdf_content:
              futures.append(executor.submit(self._handle_upload, item, pdf_content))

        for future in concurrent.futures.as_completed(futures):
          future.result()
      except Exception as e:
        raise utils.PleaseRetryException(f'Selenium crashed - {e}')

  def _handle_upload(self, item, pdf_content):
    logger.debug(f'GET {item} UPLOAD')

    if pdf_content and len(pdf_content) > 100:
      self._output.save_from_contents(
          filepath=f"{item.dest}.pdf",
          contents=pdf_content,
          content_type='application/pdf')
    else:
      logger.warn(f"Got empty document for {item.dest}.pdf")

  @utils.retryable(sleeptime=1.1, message='Could not access pdf page', ignore_if_exceeds=True)
  def download_files(self, browser, row):
    links = row.find_all('a', {'title': 'Visualizar Inteiro Teor'})
    assert len(links) == 1, f"Found {len(links)} links, expected 1."
    kvs = {k:v for k,v in zip(['cdProcesso', 'cdForo', 'nmAlias', 'cdDoc'],links[0]['name'].split('-'))}
    search_url = f"https://esaj.tjsp.jus.br/cjpg/obterArquivo.do?cdProcesso={kvs['cdProcesso']}&cdForo={kvs['cdForo']}&nmAlias={kvs['nmAlias']}&cdDocumento={kvs['cdDoc']}"
    browser.driver.execute_script(f"window.open('{search_url}')")
    browser.switch_to_window(1)
    browser.driver.implicitly_wait(30)
    if browser.bsoup().find('li', text=re.compile('.*Todos os documentos dependentes ou apensos foram omitidos para o processo\..*')):
      raise utils.PleaseRetryException(f'Documents not available for {search_url} - retrying anyway...')
    WebDriverWait(browser.driver, 30).until(EC.presence_of_element_located((By.XPATH, '//iframe[@src!="processando.html"]')))
    browser.driver.implicitly_wait(30)
    iframe = browser.bsoup().find('iframe')
    relative_url = re.match(r'.*viewer\.html\?file=(.*)$', iframe['src'])
    if relative_url is None:
      raise utils.PleaseRetryException('Could not load PDF from source')
    relative_url = relative_url.group(1)
    
    try:
      response=requests.get('https://esaj.tjsp.jus.br' + urllib.parse.unquote(relative_url),
        cookies = browser.get_cookie_dict())
    except requests.ConnectionError:
      raise utils.PleaseRetryException('Could not connect to PDF source')
    
    
    old_handle, *handles = browser.driver.window_handles
    for handle in handles:
      browser.driver.switch_to.window(handle)
      browser.driver.close()
    browser.driver.switch_to.window(old_handle)
    return response.content

@celery.task(name='tjsp1i.pdf', autoretry_for=(Exception,),
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
        base.Content(
          content=item['row'],
          dest=item['dest'])
        )
      downloader.download(to_download, pbar)


def tjsp1i_download(items, output_uri, pbar):
  output     = utils.get_output_strategy_by_path(path=output_uri)
  downloader = TJSP1IDownloader(output=output)
  to_download = []
  for _, item in enumerate(items):
    to_download.append(
        base.Content(
          content=item['row'],
          dest=item['dest'],
          content_type='text/html')
        )
  downloader.download(to_download, pbar)

@cli.command(name='tjsp1i-pdf')
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
def tjsp1i_pdf_command(input_uri, start_date, end_date, dry_run, local, count, max_workers, batch):
  # batch  = []
  global MAX_WORKERS
  MAX_WORKERS=int(max_workers)
  output = utils.get_output_strategy_by_path(path=input_uri)
  startDate = pendulum.parse(start_date)
  endDate = pendulum.parse(end_date)

  if count:
    total = 0
    while startDate <= endDate:
      for _ in list_pending_pdfs(output._bucket_name, startDate.format('YYYY/MM')):
        total += 1
      startDate = startDate.add(months=1)
    print('Total files to download', total)
    return

  while startDate <= endDate:
    print(f"TJSP1I - Collecting PDFs {startDate.format('YYYY/MM')}...")
    pendings = []
    counter = 0 
    for pending in list_pending_pdfs(output._bucket_name, startDate.format('YYYY/MM')):
      pendings.append(pending)
      counter += 1
      if counter % batch == 0:
        utils.run_pending_tasks(tjsp1i_download, pendings, input_uri=input_uri, dry_run=dry_run, max_workers=max_workers)
        pendings=[]
    utils.run_pending_tasks(tjsp1i_download, pendings, input_uri=input_uri, dry_run=dry_run, max_workers=max_workers)
    startDate = startDate.add(months=1)
