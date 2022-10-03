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
from crawlers.trf1.trf1_crawler import TRF1Client
from crawlers.trf1.trf1_utils import list_pending_pdfs
from logconfig import logger_factory

logger = logger_factory('trf1-pdf')

DEBUG = False
SITE_KEY = '6LfkZ24UAAAAAMO1KEF_pP-G3wE0dYN69-SG8NxI' # k value of recaptcha, found inside page
WEBSITE_URL = 'https://www2.cjf.jus.br/jurisprudencia/trf1/index.xhtml'
TRF1_DATE_FORMAT = 'DD/MM/YYYY'
CRAWLER_DATE_FORMAT = 'YYYY-MM-DD'
DATE_PATTERN = re.compile(r'\d{2}/\d{2}/\d{4}')
FILES_PER_PAGE = 30  #10, 30 or 50
PDF_URL = 'https://pje2g.trf1.jus.br/consultapublica/ConsultaPublica/DetalheProcessoConsultaPublica/documentoSemLoginHTML.seam'
TRF1_SEARCH_LINK = 'https://pje2g.trf1.jus.br/consultapublica/ConsultaPublica/listView.seam'
DOC_TO_PDF_CONTAINER_URL = 'http://localhost/unoconv/pdf'

class TRF1Downloader:

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

        pdf_content, inteiro_page_content = self.download_files(BeautifulSoup(item.content,'html.parser'))
        
        if pbar:
          pbar.update(1)

        # up async
        if pdf_content or inteiro_page_content:
          last_run = time.time()
          futures.append(executor.submit(self._handle_upload, item, pdf_content,inteiro_page_content))

      for future in concurrent.futures.as_completed(futures):
        future.result()

  def _handle_upload(self, item, pdf_content, inteiro_page_content):
    logger.debug(f'GET {item} UPLOAD')

    if pdf_content and len(pdf_content) > 100:
      self._output.save_from_contents(
          filepath=f"{item.dest}.pdf",
          contents=pdf_content,
          content_type='application/pdf')
    else:
      logger.warn(f"Got empty document for {item.dest}.pdf")
    
    if inteiro_page_content and len(inteiro_page_content) > 100:
      self._output.save_from_contents(
        filepath=f"{item.dest}_B.html",
        contents=inteiro_page_content,
        content_type='text/html')

    
  @utils.retryable()
  def download_files(self, row):
    
    from bs4 import BeautifulSoup
    import re
    import browsers
    
    MAXIMUM_TIME_DISTANCE = 150
    try_pje = False
    
    pdf_content, inteiro_page_content = '',''
    
    acordao_titulo = row.find(attrs={'class':"titulo_doc"}).text if row.find(attrs={'class':"titulo_doc"}) else ''
    title = re.sub(r'[^\d\-\.]+','',acordao_titulo)
    process_link = row.find('a',text='Acesse aqui')
    PUB_DATE_PATTERN = r'Data da publicação[^\d]*(?P<date>(?P<day>\d{2})\/(?P<month>\d{2})\/(?P<year>\d{4}))'
    try:
      pub_date = re.search(PUB_DATE_PATTERN, row.text.encode('latin-1').decode())
    except UnicodeDecodeError:
      pub_date = re.search(PUB_DATE_PATTERN, row.text)
    if process_link is not None:
      process_link = process_link.get('href')
    else:
      logger.warn(f'Process link not available for {title}')
      try_pje=True
    
    #ARQUIVO TRF1
    if not try_pje and 'PesquisaMenuArquivo' in process_link:

      def order_candidate(candidate):
        ordering = {'ementa':1,'relatório':2,'voto':3}
        for key in ordering:
          if key in candidate['name'].lower(): 
            return ordering[key]
        else:
          return max(ordering.values()) + 1


      import requests
      try:
        response = requests.get(process_link)
      except requests.exceptions.ConnectionError:
        raise utils.PleaseRetryException()
      soup = BeautifulSoup(response.text, 'html.parser')
      table = soup.find('table')
      if table:
        rows = soup.find_all('tr')
        candidates = [
          {
            'name':row.find_all('td')[0].text,
            'date':row.find_all('td')[1].text,
            'url': row.find_all('td')[0].find('a').get('href')
            } for row in rows[1:]
        ]
        candidates = [candidate for candidate in candidates if any(not char.isspace() for char in candidate['date'])]
        pub_date_string = f"{pub_date['day']}/{pub_date['month']}/{pub_date['year']}"
        dates = [link['date'] for link in candidates if re.search(DATE_PATTERN, link['date'])]
        nearest_date = self.get_nearest_date(dates, pub_date_string)
          
        if nearest_date:
            time_from_pub_date = abs((pendulum.from_format(pub_date_string,TRF1_DATE_FORMAT) - nearest_date).days)
        if dates and nearest_date and time_from_pub_date < MAXIMUM_TIME_DISTANCE:
            candidates = [link for link in candidates if pendulum.from_format(link['date'],TRF1_DATE_FORMAT) == nearest_date]
            candidates = sorted(candidates, key=order_candidate)
            links = [candidate['url'] for candidate in candidates]
            pdf_content = self.merge_pdfs_from_links(links, is_doc=True)
        else:
            logger.info(f"Trying to fetch {title} on PJE...")
            try_pje=True
      else:
        logger.info(f"Trying to fetch {title} on PJE...")
        try_pje=True

    # PJE
    if try_pje or 'ConsultaPublica/listView.seam' in process_link:
        browser = browsers.FirefoxBrowser(headless=not DEBUG)
        success = self.search_trf1_process_documents(browser, title)
        inteiro_soup = browser.bsoup()

        error_div = inteiro_soup.find(text=re.compile(r'.*Unhandled or Wrapper.*'))
        if not success or error_div:
            logger.warn(f'Document not available for: {title}')
        
        else:
            inteiro_page_content = browser.page_source()
            links = self.collect_all_links(browser)
            ls = []
            for link in self.filter_links(links):
                DATE_PATTERN_2 = r'.*(?P<date>\d{2}\/\d{2}\/\d{4}).*'
                LINK_PATTERN = r".*\'(?P<pdf_link>http.*?)\'.*"
                if re.search(LINK_PATTERN, link['onclick']) and re.search(DATE_PATTERN_2, link.text).group(1):
                    ls.append({
                        'date':re.search(DATE_PATTERN_2, link.text).group(1), 
                        'url': re.search(LINK_PATTERN, link['onclick']).group(1)
                        })
            
            nearest_date = self.get_nearest_date([l['date'] for l in ls], pub_date.groupdict().get('date'))
            ls = [l for l in ls if l['date'] == nearest_date.format(TRF1_DATE_FORMAT)]
            if not ls or abs(pendulum.from_format(pub_date.groupdict().get('date'), TRF1_DATE_FORMAT) - nearest_date).days > MAXIMUM_TIME_DISTANCE:
                logger.info(f'Document not available for: {title}')
                # continue
            else:
                browser.get(ls[0]['url'])
                browser.driver.implicitly_wait(20)
                pdf_content = self.download_pdf(browser)
        browser.driver.quit()

    return pdf_content, inteiro_page_content

  def get_nearest_date(self, items, pivot):
    pivot = pendulum.from_format(pivot, TRF1_DATE_FORMAT)
    if items and pivot:
        return min([pendulum.from_format(item, TRF1_DATE_FORMAT) for item in items],
                key=lambda x: abs(x - pivot))
    else:
        return ''

  @utils.retryable(max_retries=9)
  def merge_pdfs_from_links(self, document_links, is_doc=False):
      import PyPDF2 
      from io import BytesIO
      import requests
      
      TRF1_ARCHIVE = 'https://arquivo.trf1.jus.br'
      merger = PyPDF2.PdfFileMerger()
      for link in document_links:
          
          file = requests.get(f"{TRF1_ARCHIVE}{link}")
          if file.status_code == 200 and len(file.content) > 1:
              if is_doc:
                  pdf_bytes = utils.convert_doc_to_pdf(file.content, container_url=DOC_TO_PDF_CONTAINER_URL)
              pdf_bytes_io = BytesIO(pdf_bytes)
          else:
              raise utils.PleaseRetryException()
          try:
              merger.append(pdf_bytes_io)
          except PyPDF2.errors.PdfReadError:
              raise utils.PleaseRetryException()

      pdf_bytes = BytesIO()
      merger.write(pdf_bytes)
      return pdf_bytes.getvalue()

  @utils.retryable()
  def click_next_document_page(self, browser, slider_id, page):
    from selenium.common.exceptions import StaleElementReferenceException, NoSuchElementException
    try:
        slider_page_input = browser.driver.find_element_by_id(slider_id)
        browser.driver.execute_script(f"arguments[0].value =  {page};", slider_page_input);
        browser.driver.execute_script("A4J.AJAX.Submit('j_id141:j_id633',event,{'similarityGroupingId':'j_id141:j_id633:j_id635','actionUrl':'/consultapublica/ConsultaPublica/DetalheProcessoConsultaPublica/listView.seam','eventsQueue':'','containerId':'j_id141:j_id549','parameters':{'j_id141:j_id633:j_id635':'j_id141:j_id633:j_id635'},'status':'_viewRoot:status'} )");
        browser.driver.implicitly_wait(10)
    except StaleElementReferenceException:
        browser.driver.refresh()
        raise utils.PleaseRetryException()
    except NoSuchElementException:
        return
    except Exception as e:
        logger.warn(f'Something went wrong clicking slider on {browser.current_url()}, retrying...')
        raise utils.PleaseRetryException()

  def collect_all_links(self,browser):
    from bs4 import BeautifulSoup
    import re
    links = []
    while True:
        soup = BeautifulSoup(browser.page_source(),'html.parser')
        table = soup.find(id=re.compile(r'j_id\d+:processoDocumentoGridTabPanel_body'))
        hypers = table.find_all('a')
        links.extend(hypers)
        last_page = table.find('td', class_='rich-inslider-right-num')
        if not last_page:
            break
        last_page = int(last_page.text)
        current_page = table.find('input',attrs={'class':"rich-inslider-field-right rich-inslider-field"})
        current_page = int(current_page['value'])
        
        if last_page and not last_page < current_page + 1:
            self.click_next_document_page(browser=browser, 
                slider_id='j_id141:j_id633:j_id634Input', page=current_page+1)
            browser.driver.implicitly_wait(20)
        else:
            break
    return links

  def filter_links(self, links):
    import re
    links = [link for link in links if re.search(r'\d{2}',link.text)]
    links = [link for link in links if re.search('Acórdão', link.text, re.IGNORECASE + re.UNICODE)]
    links = list(set(links))
    return links


  @utils.retryable(max_retries=9, sleeptime=20)
  def search_trf1_process_documents(self, browser, title):
      from selenium.webdriver.support.ui import WebDriverWait
      from selenium.webdriver.support import expected_conditions as EC
      from selenium.webdriver.common.by import By
      from selenium.common.exceptions import TimeoutException, WebDriverException

      browser.get(TRF1_SEARCH_LINK)
      browser.fill_in('fPP:numProcesso-inputNumeroProcessoDecoration:numProcesso-inputNumeroProcesso', title)
      browser.driver.implicitly_wait(20)
      WebDriverWait(browser.driver, 20).until(EC.element_to_be_clickable((By.ID,'fPP:searchProcessos'))).click() 
      try:
          WebDriverWait(browser.driver, 20).until(EC.presence_of_element_located((By.XPATH, '//a[@title="Ver Detalhes"]')))
      except TimeoutException:
          return False
      link = browser.bsoup().find_all('a', attrs={'title':'Ver Detalhes'})
      if len(link) != 1:
          return False
      link = re.search(r".*\(\'Consulta pública\','(.*?)\'\)",link[0]['onclick']).group(1)
      try:
        browser.get(f'https://pje2g.trf1.jus.br{link}')
      except WebDriverException:
        raise utils.PleaseRetryException()
      browser.driver.implicitly_wait(20)
      return True

  @utils.retryable(max_retries=9, sleeptime=20)
  def download_pdf(self, browser):
      """Download PDF as bytes when browser is in the page where the "Gerar PDF" button is available"""
      import requests
      link_container =  browser.bsoup().find('a',id='j_id47:downloadPDF')
      DATA_PATTERN = r".*\'ca\'\:\'(?P<ca>.*)\',\'idProcDocBin\'\:\'(?P<idProcDocBin>\d+)\'.*"
      page_data = re.search(DATA_PATTERN, link_container['onclick']).groupdict()

      cookies = {
          'JSESSIONID': browser.get_cookie('JSESSIONID')
      }

      data = {
          'j_id47': 'j_id47',
          'javax.faces.ViewState': browser.bsoup().find("input", {"type": "hidden", "name":"javax.faces.ViewState"})['value'],
          'j_id47:downloadPDF': 'j_id47:downloadPDF',
          'ca': page_data['ca'],
          'idProcDocBin': page_data['idProcDocBin'],
      }

      headers = {
          'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:99.0) Gecko/20100101 Firefox/99.0',
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
          'Accept-Language': 'en-US,en;q=0.5',
          # 'Accept-Encoding': 'gzip, deflate, br',
          'Origin': 'https://pje2g.trf1.jus.br',
          'Connection': 'keep-alive',
          'Referer': f'https://pje2g.trf1.jus.br/consultapublica/ConsultaPublica/DetalheProcessoConsultaPublica/documentoSemLoginHTML.seam?ca={page_data["ca"]}&idProcessoDoc={page_data["idProcDocBin"]}',
          'Upgrade-Insecure-Requests': '1',
          'Sec-Fetch-Dest': 'document',
          'Sec-Fetch-Mode': 'navigate',
          'Sec-Fetch-Site': 'same-origin',
          'Sec-Fetch-User': '?1',
      }
      response = requests.post(PDF_URL, cookies=cookies, headers=headers, data=data)
      if response.status_code == 200 and len(response.text) > 50:
          return response.content
      else:
          raise utils.PleaseRetryException()

@celery.task(queue='trf1.pdf', autoretry_for=(Exception,),
             default_retry_delay=60, max_retries=3)
def trf1_download_task(items, output_uri):
  from tqdm import tqdm

  time.sleep(random.uniform(5., 15.))

  output = utils.get_output_strategy_by_path(path=output_uri)
  downloader = TRF1Downloader(output)

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


def trf1_download(items, output_uri, pbar):
  output     = utils.get_output_strategy_by_path(path=output_uri)
  downloader = TRF1Downloader(output=output)
  to_download = []
  for n, item in enumerate(items):
    to_download.append(
        base.Content(
          content=item['row'], 
          dest=item['dest'],
          content_type='text/html')
        )
  downloader.download(to_download, pbar)

@cli.command(name='trf1-pdf')
@click.option('--prefix')
@click.option('--input-uri'   , help='Input URI')
@click.option('--dry-run'     , default=False, is_flag=True)
@click.option('--local'       , default=False, is_flag=True)
@click.option('--count'       , default=False, is_flag=True)
def trf1_pdf_command(input_uri, prefix, dry_run, local, count):
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
            futures.append(executor.submit(trf1_download, batch, input_uri, pbar))
            # time.sleep(random.uniform(5., 8.))
            batch = []

    print("Tasks distributed -- waiting for results")
    for future in concurrent.futures.as_completed(futures):
      future.result()
    executor.shutdown()
    if len(batch):
      trf1_download(batch, input_uri, pbar)