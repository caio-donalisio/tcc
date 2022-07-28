import logging
import pathlib
import random
import time

import base
import click
import pendulum
import utils
from app import celery, cli
from crawlers.trf5.trf5_crawler import TRF5Client
from crawlers.trf5.trf5_utils import list_pending_pdfs
from logconfig import logger_factory

logger = logger_factory('trf5-pdf')

MAX_WORKERS = 8


class TRF5Downloader:

  def __init__(self, client=None, output=None):
    self._client = client
    self._output = output

  def download(self, items, pbar=None):
    import concurrent.futures
    import time
    # self._client.signin()
    # self._client.set_search(
    #   start_date=pendulum.DateTime(2020, 1, 1),
    #   end_date=pendulum.DateTime(2020, 1, 31),
    # )
    # self._client.close()

    interval = 5
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
      futures  = []
      last_run = None
      for item in items:
        # request sync
        # now = time.time()
        # if last_run is not None:
        #   since = now - last_run
        #   if since < interval:
        #     jitter     = random.uniform(.5, 1.2)
        #     sleep_time = (interval - since) + jitter
        #     time.sleep(sleep_time)
        
      
        report = self._get_report_url(item.content)
        response = self._get_response(base.ContentFromURL(
              src=report.get('url'),
              dest=item.dest,
              content_type=report.get('content_type'),

              ))
        if pbar:
          pbar.update(1)

        # up async
        if response:
          # last_run = time.time()
          futures.append(executor.submit(self._handle_upload, item, response))

      for future in concurrent.futures.as_completed(futures):
        future.result()


  def _get_report_url(self, record: dict):
    import re

    report_url = None
    content_type_report = "text/html"

    if re.search('www4.trf5.jus.br\/processo', record['url']):
        if not self.filters('skip_full'):
            report_url = self._get_report_url_from_trf5(record)
        if report_url is None:
            report_url = self._get_report_url_from_trf5(record, digits=2)
        if report_url is not None:
            content_type_report = "application/pdf"
    else:
        report_url = self._get_report_url_from_pje(record)
    return {
      'url':report_url,
      'content_type':content_type_report
      }

  def _get_report_url_from_trf5(self, doc:dict, digits=0):
    import requests
    import re
    from bs4 import BeautifulSoup

    judgment_date = pendulum.parse(doc['dataJulgamento'])
    judgment_id = doc['numeroProcesso'][0:len(doc['numeroProcesso'])-digits]

    data = {
        'numproc': judgment_id,
    }
    baseURL = 'https://www4.trf5.jus.br'
    response = requests.post(f'{baseURL}/InteiroTeor/publicacoes.jsp', data=data)
    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find('table', {'cellpadding': '5'})
    pattern = "(\d{2})\/(\d{2})\/(\d{4})\s*.*.pdf"
    trs = table.find_all('tr')
    links = []
    for tr in trs:
        match = re.search(pattern, tr.text)
        if match:
            doc_date = pendulum.parse(f'{match.group(3)}-{match.group(2)}-{match.group(1)}')
            days = doc_date.diff(judgment_date).in_days()
            a = tr.find('a')
            if days >= 0 and a:
                links.append({
                    'days': days,
                    'url': f"{baseURL}{a.get('href')}"
                })

    return self._get_judgment_doc_url_by_closest_date(links)

    
  def _get_report_url_from_pje(self, doc):
    import browsers

    browser = browsers.FirefoxBrowser(headless=True)
    details_url = self._get_judgment_details_url(browser, doc)
    if not details_url:
      return None
    return self._get_judgment_doc_url(details_url, browser, doc)
    

  def _get_judgment_details_url(self, browser, doc):
    from selenium.webdriver.common.by import By

    browser.get(doc['url'])

    browser.wait_for_element(locator=(By.ID, 'consultaPublicaForm:captcha:captchaImg'), timeout=30)

    judgment_id = self._format_process_number(doc['numeroProcesso'])
    logger.info(judgment_id)
    process_input = browser.driver.find_element_by_id('consultaPublicaForm:Processo:ProcessoDecoration:Processo')  
    browser.driver.execute_script(f"arguments[0].value='{judgment_id}';", process_input)

    while not browser.is_text_present('Ver Detalhes', tag='img'):
        logger.debug(f'Solving Captcha...')
        captcha_img = browser.driver.find_element_by_id('consultaPublicaForm:captcha:captchaImg')

        captcha_img_base64 = browser.driver.execute_script("""
            var ele = arguments[0];
            var cnv = document.createElement('canvas');
            cnv.width = ele.width; cnv.height = ele.height;
            cnv.getContext('2d').drawImage(ele, 0, 0);
            return cnv.toDataURL('image/jpeg').substring(23);    
            """, captcha_img)

        captcha_resolved = self._resolve_captcha(captcha_img_base64)
        logger.debug(f'Captcha API Resolved: {captcha_resolved}')
        captcha_input = browser.driver.find_element_by_id('consultaPublicaForm:captcha:j_id268:verifyCaptcha')
        captcha_input.send_keys(captcha_resolved)

        search_button = browser.driver.find_element_by_id('consultaPublicaForm:pesq')
        search_button.click()
        time.sleep(1)
    
        if not browser.is_text_present('Resposta incorreta'):
          if browser.is_text_present('Foram encontrados: 0 resultados'):
            logger.warn(f'Not results for {judgment_id}')
            return None
          browser.wait_for_element(locator=(By.ID, 'consultaPublicaList2:0:j_id315:j_id318'), timeout=60)
          doc_link = browser.driver.find_element_by_id('consultaPublicaList2:0:j_id315:j_id318')                                            
          return self._extract_judgment_detail_url(doc_link)
        else:
            logger.warn(f'Incorrect Captcha!!! Trying again...')

    
  def _format_process_number(self, value):
    import re

    value = "{:0>20}".format(int(value))
    return re.sub("(\d{7})(\d{2})(\d{4})(\d{1})(\d{2})(\d{4})",
                "\\1-\\2.\\3.\\4.\\5.\\6",
                value)

    
  def _resolve_captcha(self, captcha):
    import requests
    import os

    api_key = os.getenv('CAPTCHA_API_KEY')

    post_data = {
        'action':'upload',
        'key': api_key,
        'captchatype': 2,
        'gen_task_id': f'{int(time.time())}',
        'file': captcha
    }

    captcha_api_url = 'http://fasttypers.org/Imagepost.ashx'
    logger.debug(f'(Captcha) POST {captcha_api_url}')
    r  = requests.post(captcha_api_url, data=post_data)
    logger.debug(f'(Captcha) Response: {r.text}')

    return r.text


  def _extract_judgment_detail_url(self, doc_link):
    import re

    event_data = doc_link.get_attribute('onclick')
    pattern = re.compile(r"openPopUp\('\d+popUpDetalhesProcessoConsultaPublica', '(.*)'\);")
    m = pattern.match(event_data)
    if m:
        return f"https://pje.trf5.jus.br{m.group(1)}"

    return None


  def _get_judgment_doc_url(self, url: str, browser, doc):
    import re
    from bs4 import BeautifulSoup
    from selenium.webdriver.common.by import By

    judgment_date = pendulum.parse(doc['dataJulgamento'])
    pattern = '(\d{2})\/(\d{2})\/(\d{4})\s(\d{2})\:(\d{2})\:(\d{2})\s- Inteiro Teor - Inteiro Teor do Acórdão'
    browser.get(url)
    browser.driver.maximize_window()
    time.sleep(0.5)
    browser.wait_for_element(locator=(By.ID, 'processoEvento'), timeout=60)

    slider_page = 1
    slider_total_pages = 1
    if browser.driver.find_elements(By.XPATH, "//div[contains(@class, 'rich-inslider-handler')]"):
        slider = browser.driver.find_element(By.XPATH, "//div[contains(@class, 'rich-inslider-handler')]")
        browser.driver.execute_script("arguments[0].scrollIntoView()", slider);

        slider_total_pages_td = browser.driver.find_element(By.XPATH, "//td[contains(@class, 'rich-inslider-right-num')]")
        slider_total_pages = int(slider_total_pages_td.text)

        slider_page_input = browser.driver.find_element_by_id('j_id423:j_id424Input')
        slider_page = int(slider_page_input.get_attribute('value'))

    links = []
    while slider_page <= slider_total_pages:
        html = browser.driver.page_source
        soup = BeautifulSoup(html, features='html.parser')
        table = soup.find("table", {"id": "processoEvento"})
        tds = table.find_all("td", {"class": "rich-table-cell"})
        for td in tds:
            match = re.search(pattern, td.text)
            if match:
                a = td.find('a')
                doc_date = pendulum.parse(f'{match.group(3)}-{match.group(2)}-{match.group(1)}')
                days = doc_date.diff(judgment_date).in_days()
                if days >= 0 and a:
                    links.append({
                        'days': days,
                        'url': self._extract_url_from_event(a.get('onclick'))
                    })
                    
        if browser.driver.find_elements_by_id('j_id423:j_id424Input'):
            slider_page_input = browser.driver.find_element_by_id('j_id423:j_id424Input')
            browser.driver.execute_script("arguments[0].value = Number(arguments[0].value) + 1;", slider_page_input);
            slider_page = int(slider_page_input.get_attribute('value'))
            browser.driver.execute_script("A4J.AJAX.Submit('j_id423',event,{'similarityGroupingId':'j_id423:j_id425','actionUrl':'/pjeconsulta/ConsultaPublica/DetalheProcessoConsultaPublica/listView.seam','eventsQueue':'default','containerId':'j_id340','parameters':{'j_id423:j_id425':'j_id423:j_id425'} ,'status':'_viewRoot:status'} )");
            time.sleep(2)
        else:
            slider_page += 1

    return self._get_judgment_doc_url_by_closest_date(links)

    
  def _get_judgment_doc_url_by_closest_date(self, links):
    if len(links) > 0:
        sorted_list = sorted(links, key=lambda d: d['days'])
        return sorted_list[0]['url']
    
    return None

  def _extract_url_from_event(self, event_data):
    import re

    pattern = re.compile(r"openPopUp\('PopUpDocumentoBin', '(.*)'\);")
    m = pattern.match(event_data)
    if m:
        return m.group(1)
    
    return None


  @utils.retryable(max_retries=3)
  def _get_response(self, content_from_url):
    import requests
    logger.debug(f'GET {content_from_url.src}')
    # for cookie in self._client.request_cookies_browser:
    #   self._client.session.cookies.set(cookie['name'], cookie['value'])

    if self._output.exists(content_from_url.dest):
      return None

    response = requests.Session().get(content_from_url.src,
      # headers=self._client.header_generator.generate(),
      allow_redirects=True,
      verify=False,
      timeout=10)
    if 'application/pdf' in response.headers.get('Content-type'):
      logger.info(f'Code {response.status_code} (OK) for URL {content_from_url.src}.')
      # return response
    elif 'text/html' in response.headers.get('Content-type') and \
      not response.content:
      logger.warn(f'HTML for {content_from_url.src} not available.')
    else:
      logger.info(f'Code {response.status_code} for URL {content_from_url.src}.')
      if response.status_code != 200:
        logger.warn(
        f"Got {response.status_code} when fetching {content_from_url.src}. Content-type: {response.headers.get('Content-type')}.")
        raise utils.PleaseRetryException()
      else:
        return response

  def _handle_upload(self, content_from_url, response):
    # logger.debug(f'GET {content_from_url.src} UPLOAD')
    if 'pdf' in content_from_url.content_type:
      filepath = f'{content_from_url.dest}.pdf'
    elif 'html' in content_from_url.content_type:
      filepath = f'{content_from_url.dest}.html'
    
    if len(response.content) > 0:
      self._output.save_from_contents(
          filepath=filepath,
          contents=response.content,
          content_type=content_from_url.content_type)
    else:
      logger.warn(
        f"Got 0 bytes for {content_from_url.src}. Content-type: {response.headers.get('Content-type')}.")


@celery.task(queue='trf5.pdf', autoretry_for=(Exception,),
             default_retry_delay=60, max_retries=3)
def trf5_download_task(items, output_uri):
  from tqdm import tqdm

  time.sleep(random.uniform(5., 15.))

  output     = utils.get_output_strategy_by_path(path=output_uri)
  client     = TRF5Client()
  downloader = TRF5Downloader(client=client, output=output)

  tqdm_out = utils.TqdmToLogger(logger, level=logging.INFO)

  with tqdm(total=len(items), file=tqdm_out) as pbar:
    downloader.download(
      [
        base.ContentFromURL(
          row=item['url'],
          dest=item['dest'],
          content_type='application/doc'
        )
        for item in items
      ],
      pbar
    )


def trf5_download(items, output_uri, pbar):
  output     = utils.get_output_strategy_by_path(path=output_uri)
  client     = TRF5Client()
  downloader = TRF5Downloader(client=client, output=output)
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


@cli.command(name='trf5-pdf')
@click.option('--prefix')
@click.option('--input-uri'   , help='Input URI')
@click.option('--dry-run'     , default=False, is_flag=True)
@click.option('--local'       , default=False, is_flag=True)
@click.option('--count'       , default=False, is_flag=True)
def trf5_pdf_command(input_uri, prefix, dry_run, local, count):
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
    for pending in list_pending_pdfs(output._output_folder, prefix):
      pendings.append(pending)

    with tqdm(total=len(pendings)) as pbar:
      executor = concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS)
      futures  = []
      for pending in pendings:
        if not dry_run:
          batch.append(pending)
          if len(batch) >= 5:
            futures.append(executor.submit(trf5_download, batch, input_uri, pbar))
            # time.sleep(random.uniform(5., 8.))
            batch = []

    print("Tasks distributed -- waiting for results")
    for future in concurrent.futures.as_completed(futures):
      future.result()
    executor.shutdown()
    if batch:
      trf5_download(batch, input_uri, pbar)

  else:
    total = 0
    for pending in list_pending_pdfs(output._bucket_name, prefix):
      total += 1
      batch.append(pending)
      if len(batch) >= 100:
        print("Task", trf5_download_task.delay(batch, input_uri))
        batch = []
    if len(batch):
      print("Task", trf5_download_task.delay(batch, input_uri))
    print('Total files to download', total)
