import logging
import random
import time
from app.crawlers import base, utils

import click
import pendulum

from app.celery_run import celery_app as celery
from app.crawler_cli import cli

from app.crawlers.cmtsp.cmtsp_crawler import CMTSPClient
from app.crawlers.cmtsp.cmtsp_utils import list_pending_pdfs
from app.crawlers.logconfig import logger_factory
from bs4 import BeautifulSoup

from selenium.webdriver.common.by import By

logger = logger_factory('cmtsp-pdf')

CMTSP_DATE_FORMAT = 'DDMMYYYY'
MAX_WORKERS=3
SITE_KEY = '6Lf778wZAAAAAKo4YvpkhvjwsrXd53EoJOWsWjAY' # k value of recaptcha, found inside page
WEBSITE_URL = 'http://sagror.prefeitura.sp.gov.br/ManterDecisoes/pesquisaDecisoesCMT.aspx'
PDF_URL = 'http://sagror.prefeitura.sp.gov.br/ManterDecisoes/VisualizarArquivo.aspx'
DEFAULT_HEADERS =  {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36 Edg/107.0.1418.56',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6,ja;q=0.5',
            'Cache-Control': 'max-age=0',
            # 'Accept-Encoding': 'gzip, deflate',
            'Referer': 'http://sagror.prefeitura.sp.gov.br/ManterDecisoes/pesquisaDecisoesCMT.aspx',
            'Connection': 'keep-alive',
            # Requests sorts cookies= alphabetically
            # 'Cookie': 'ASP.NET_SessionId=pl0zpa11y03mwmshi1inotwv; SWCookieConfig={"aceiteSessao":"S","aceitePersistentes":"N","aceiteDesempenho":"N","aceiteEstatisticos":"N","aceiteTermos":"S"}',
            'Upgrade-Insecure-Requests': '1',
        }


class CMTSPDownloader:

  def __init__(self, output):
    # self._client = client
    self._output = output

  def download(self, items, pbar=None):
    import concurrent.futures
    import time

    interval = 5
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
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

        # pdf_content = self.get_pdf_content(BeautifulSoup(item.content,'html.parser'))

        if pbar:
          pbar.update(1)

        # up async
        # if pdf_content:
        last_run = time.time()
        futures.append(executor.submit(self._handle_upload, item))#, pdf_content))

      for future in concurrent.futures.as_completed(futures):
        future.result()

  @utils.retryable(ignore_if_exceeds=True)
  def _handle_upload(self, item):
    pdf_content = self.get_pdf_content(BeautifulSoup(item.content,'html.parser'))
    logger.debug(f'GET {item} UPLOAD')

    if pdf_content and len(pdf_content) > 50_000:
      self._output.save_from_contents(
          filepath=f"{item.dest}.pdf",
          contents=pdf_content,
          content_type='application/pdf')
    else:
      logger.warn(f"Got empty document for {item.dest}.pdf - retrying...")
      raise utils.PleaseRetryException()


  @utils.retryable(ignore_if_exceeds=True)
  def get_pdf_content(self, row):
    process, _, date , __ = self.extract_info_from_meta(row)
    rows = []
    page = 1
    client = CMTSPClient()
    client.setup()

    #Try searching by process number:
    filters = {'process':process}
    client.make_search(filters=filters, by='process')
    rows = self.filter_rows(reference_row=row, rows=client.browser.bsoup().find_all('tr'))

    # Retry searching by date if not found:
    if not rows:
      logger.info(f"Couldn't find process {process} by process number, searching by date...")
      date_obj = pendulum.from_format(utils.extract_digits(date),'YYYYMMDD')
      filters = {
        'start_date': date_obj.format('YYYY-MM-DD'),
        'end_date': date_obj.add(days=1).format('YYYY-MM-DD')}
      client.browser.driver.close()
      client.setup()
      client.make_search(filters=filters, by='date')
      rows = self.filter_rows(reference_row=row, rows=client.browser.bsoup().find_all('tr'))
      if not rows:
        while not client.search_is_over(client.get_current_page()):
          if len(rows) > 0:
            break
          else:
            page += 1
            client.fetch(filters, page=page)
            rows = self.filter_rows(reference_row=row, rows=client.browser.bsoup().find_all('tr'))

    if len(rows) == 0:
      logger.warn(f'Could not find searched process: {process}')
      client.browser.driver.quit()
    elif len(rows) == 1:
      pdf_content = self.fetch_pdf(self.get_pdf_session_id(client.browser, rows[0]))
      client.browser.driver.quit()
      return pdf_content
    elif len(rows) > 1:
      client.browser.driver.quit()
      logger.warn(f'{len(rows)} processes found for {process}, expected 1')
      # raise Exception(f'{len(rows)} processes found for {process}, expected 1')

  def extract_info_from_meta(self, row):
    process = row.find_all('td')[0].text.strip()
    camara = row.find_all('td')[1].text.strip()
    date = row.find_all('td')[2].text.strip()
    ementa = row.find_all('td')[3].text.strip()
    return (process, camara, date, ementa)

  def filter_rows(self, reference_row, rows):
    process, camara, _, ementa = self.extract_info_from_meta(reference_row)
    rows = [tr for tr in rows if tr.find_all('td')[0].text.strip() == process]
    rows = [tr for tr in rows if tr.find_all('td')[1].text.strip() == camara]
    rows = [tr for tr in rows if tr.find_all('td')[2].text.strip() == ementa]
    return rows

  @utils.retryable(ignore_if_exceeds=True)
  def get_pdf_session_id(self, browser, tr):
    browser.driver.find_element(By.ID, tr.a['id']).click()
    browser.driver.implicitly_wait(3)
    try:
      main_window, pop_up_window = browser.driver.window_handles
    except ValueError:
      raise utils.PleaseRetryException()
    browser.driver.switch_to.window(pop_up_window)
    browser.driver.implicitly_wait(10)
    if browser.bsoup().find('div', class_='g-recaptcha'):
        raise Exception('Captcha not expected')
    browser.driver.close()
    browser.driver.switch_to.window(main_window)
    session_id = browser.get_cookie('ASP.NET_SessionId')
    browser.driver.quit()
    return session_id

  @utils.retryable(ignore_if_exceeds=True)
  def fetch_pdf(self, session_id):
      import requests
      cookies = {
          'ASP.NET_SessionId': session_id,
          'SWCookieConfig': '{"aceiteSessao":"S","aceitePersistentes":"N","aceiteDesempenho":"N","aceiteEstatisticos":"N","aceiteTermos":"S"}',
      }
      headers = DEFAULT_HEADERS
      response = requests.get(PDF_URL, cookies=cookies, headers=headers)
      return response.content

@celery.task(queue='cmtsp.pdf', autoretry_for=(Exception,),
             default_retry_delay=60, max_retries=3)
def cmtsp_download_task(items, output_uri):
  from tqdm import tqdm

  time.sleep(random.uniform(5., 15.))

  output = utils.get_output_strategy_by_path(path=output_uri)
  downloader = CMTSPDownloader(output)

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


def cmtsp_download(items, output_uri, pbar):
  output     = utils.get_output_strategy_by_path(path=output_uri)
  downloader = CMTSPDownloader(output=output)
  to_download = []
  for n, item in enumerate(items):
    to_download.append(
        base.Content(
          content=item['row'],
          dest=item['dest'],
          content_type='text/html')
        )
  downloader.download(to_download, pbar)

@cli.command(name='cmtsp-pdf')
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
def cmtsp_pdf_command(input_uri, start_date, end_date, dry_run, local, count):
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

  def run_tasks(pendings):
    batch = []
    with tqdm(total=len(pendings)) as pbar:
      executor = concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS)
      futures  = []
      for pending in pendings:
        if not dry_run:
          batch.append(pending)
          if len(batch) >= 5:
            futures.append(executor.submit(cmtsp_download, batch, input_uri, pbar))
            # time.sleep(random.uniform(5., 8.))
            batch = []

    print("Tasks distributed -- waiting for results")
    for future in concurrent.futures.as_completed(futures):
      future.result()
    executor.shutdown()
    if len(batch):
      cmtsp_download(batch, input_uri, pbar)

  if local:
    total = 0
    import concurrent.futures
    from tqdm import tqdm
    while startDate <= endDate:
      print(f"CMTSP - Collecting {startDate.format('YYYY/MM')}...")
      pendings = []
      for pending in list_pending_pdfs(output._bucket_name, startDate.format('YYYY/MM')):

        pendings.append(pending)
        total += 1

      run_tasks(pendings)
      startDate = startDate.add(months=1)
