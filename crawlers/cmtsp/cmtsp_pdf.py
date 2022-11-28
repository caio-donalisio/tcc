import logging
import pathlib
import random
import time
import re
import base
import click
import pendulum
import utils
from app import celery, cli
from crawlers.cmtsp.cmtsp_crawler import CMTSPClient
from crawlers.cmtsp.cmtsp_utils import list_pending_pdfs
from logconfig import logger_factory
import captcha
from browsers import FirefoxBrowser

logger = logger_factory('cmtsp-pdf')

DEBUG = False
SITE_KEY = '6Lf778wZAAAAAKo4YvpkhvjwsrXd53EoJOWsWjAY' # k value of recaptcha, found inside page
WEBSITE_URL = 'http://sagror.prefeitura.sp.gov.br/ManterDecisoes/pesquisaDecisoesCMT.aspx'
CMTSP_DATE_FORMAT = 'DD/MM/YYYY'
CRAWLER_DATE_FORMAT = 'YYYY-MM-DD'
DATE_PATTERN = re.compile(r'\d{2}/\d{2}/\d{4}')
# FILES_PER_PAGE = 30  #10, 30 or 50
PDF_URL = 'http://sagror.prefeitura.sp.gov.br/ManterDecisoes/VisualizarArquivo.aspx'
# CMTSP_SEARCH_LINK = 'https://pje2g.cmtsp.jus.br/consultapublica/ConsultaPublica/listView.seam'
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
    from bs4 import BeautifulSoup

    interval = 5
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
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

        pdf_content = self.get_pdf_content(BeautifulSoup(item.content,'html.parser'))
        
        if pbar:
          pbar.update(1)

        # up async
        if pdf_content:
          last_run = time.time()
          futures.append(executor.submit(self._handle_upload, item, pdf_content))

      for future in concurrent.futures.as_completed(futures):
        future.result()

  def _handle_upload(self, item, pdf_content):
    logger.debug(f'GET {item} UPLOAD')

    if pdf_content and len(pdf_content) > 100:
      self._output.save_from_contents(
          filepath=f"{item.dest}.pdf",
          contents=pdf_content,
          content_type='application/pdf')
    else:
      logger.warn(f"Got empty document for {item.dest}.pdf")

  @utils.retryable()
  def get_pdf_content(self, row):

      process = row.find_all('td')[0].text
      camara = row.find_all('td')[1].text
      ementa = row.find_all('td')[3].text

      self.browser = FirefoxBrowser()
      self.browser.get(WEBSITE_URL)
      self.browser.driver.implicitly_wait(10)
      logger.info(f'Trying to collect {process.strip()} ...')
      if self.browser.bsoup().find('prodamsp-componente-consentimento'):
          self.browser.driver.execute_script('''
          document.querySelector("prodamsp-componente-consentimento").shadowRoot.querySelector("input[class='cc__button__autorizacao--all']").click()''')
      self.browser.driver.implicitly_wait(10)

      self.browser.fill_in("txtExpressao", process.strip())
      self.browser.driver.implicitly_wait(10)
      captcha.solve_recaptcha(self.browser, logger, site_key=SITE_KEY)
      self.browser.driver.find_element_by_id('btnPesquisar').click()
      self.browser.driver.implicitly_wait(10)

      trs = self.browser.bsoup().find_all('tr')
      trs = [tr for tr in trs if tr.find_all('td')[0].text.strip() == process.strip()]
      trs = [tr for tr in trs if tr.find_all('td')[1].text.strip() == camara.strip()]
      trs = [tr for tr in trs if tr.find_all('td')[2].text.strip() == ementa.strip()]
      assert len(trs) == 1, 'Expected one line'
      return self.fetch_pdf(self.get_pdf_session_id(trs[0]))

  @utils.retryable()
  def get_pdf_session_id(self, tr):
    self.browser.driver.find_element_by_id(tr.a['id']).click()
    self.browser.driver.implicitly_wait(3)
    main_window, pop_up_window = self.browser.driver.window_handles
    self.browser.driver.switch_to_window(pop_up_window)
    self.browser.driver.implicitly_wait(10)
    if self.browser.bsoup().find('div', class_='g-recaptcha'):
        raise Exception('Captcha not expected')
    # while self.browser.bsoup().find('div', class_='g-recaptcha'):
    #     captcha.solve_recaptcha(self.browser, logger, SITE_KEY)
    #     self.browser.driver.find_element_by_id('btnVerificar').click()
    #     self.browser.driver.implicitly_wait(3)
    # session_id = self.browser.get_cookie('ASP.NET_SessionId')
    self.browser.driver.close()
    self.browser.driver.switch_to_window(main_window)
    session_id = self.browser.get_cookie('ASP.NET_SessionId')
    return session_id
    # self.browser.click()
    ...
  @utils.retryable()
  def fetch_pdf(self, session_id):
      import requests
      #self.browser.get_cookie('ASP.NET_SessionId')
      cookies = {
          'ASP.NET_SessionId': session_id,
          'SWCookieConfig': '{"aceiteSessao":"S","aceitePersistentes":"N","aceiteDesempenho":"N","aceiteEstatisticos":"N","aceiteTermos":"S"}',
      }
      headers = DEFAULT_HEADERS
      response = requests.get(PDF_URL, cookies=cookies, headers=headers)
      return response.content

    
  # @utils.retryable()
  # def download_files(self, row):
  #   client = CMTSPClient()
  #   client.setup()
  #   return client.get_pdf_content(row)

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
@click.option('--prefix')
@click.option('--input-uri'   , help='Input URI')
@click.option('--dry-run'     , default=False, is_flag=True)
@click.option('--local'       , default=False, is_flag=True)
@click.option('--count'       , default=False, is_flag=True)
def cmtsp_pdf_command(input_uri, prefix, dry_run, local, count):
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
    import concurrent.futures

    from tqdm import tqdm

    # just to count
    pendings = []
    for pending in list_pending_pdfs(output._bucket_name, prefix):
      pendings.append(pending)

    with tqdm(total=len(pendings)) as pbar:
      executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)
      futures  = []
      for pending in pendings:
        if not dry_run:
          batch.append(pending)
          if len(batch) >= 10:    
            futures.append(executor.submit(cmtsp_download, batch, input_uri, pbar))
            # time.sleep(random.uniform(5., 8.))
            batch = []

    print("Tasks distributed -- waiting for results")
    for future in concurrent.futures.as_completed(futures):
      future.result()
    executor.shutdown()
    if len(batch):
      cmtsp_download(batch, input_uri, pbar)