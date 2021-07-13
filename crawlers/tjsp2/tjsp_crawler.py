import re
import os
import json
import time
import requests
import pendulum
import utils
import random
import logging
from tqdm import tqdm
from slugify import slugify

from selenium.webdriver.support.ui import WebDriverWait

from urllib3.exceptions import InsecureRequestWarning

from utils import PleaseRetryException
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

import click
from app import cli, celery
from logconfig import logger_factory


class tjsp:
  def __init__(self, params, output, logger, **options):
    self.params = params
    self.output = output
    self.logger = logger
    self.options = (options or {})
    self.header_generator = utils.HeaderGenerator(
      origin='https://esaj.tjsp.jus.br', xhr=False)
    self.session = requests.Session()
    self.request_cookies_browser = []

  def run(self):
    import concurrent.futures

    self.signin()

    total_records = self.count()
    self.logger.info(f'Expects {total_records} records.')
    records_fetch = 0

    tqdm_out = utils.TqdmToLogger(self.logger, level=logging.INFO)
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
      with tqdm(total=total_records, file=tqdm_out) as pbar:
        for chunk in self.chunks():
          if chunk.commited():
            chunk_records  = chunk.get_value('records')
            records_fetch += chunk_records
            pbar.set_postfix(chunk.params)
            pbar.update(chunk_records)
            self.logger.debug(f"Chunk {chunk.hash} already commited ({chunk_records} records) -- skipping.")
            continue

          chunk_records = 0
          futures = []
          for html, json, pdf in chunk.rows():
            chunk_records += 1
            futures.extend([
              executor.submit(self.persist, html, content_type='text/html'),
              executor.submit(self.persist, json, content_type='application/json'),
            ])
            # We have to download pdfs sync due to the rate limiter.
            # This slows down the process a lot -- nothing to do for now.
            if self.options.get('skip_pdf', False) == False:
              pdf['source'] = self.download_pdf(pdf)
              time.sleep(random.uniform(0.5, 1.0))
              # Then, persist.
              if pdf['source'] is not None:
                futures.append(
                  executor.submit(self.persist, pdf, mode='wb', content_type='application/pdf'))

          for future in concurrent.futures.as_completed(futures):
            future.result()

          chunk.set_value('records', chunk_records)
          chunk.commit()
          records_fetch += chunk_records
          pbar.set_postfix(chunk.params)
          pbar.update(chunk_records)
          self.logger.debug(f'Chunk {chunk.hash} ({chunk_records} records) commited.')

          # time.sleep(random.uniform(0.1, 0.2))

      self.logger.info(f'Expects {total_records}. Fetched {records_fetch}.')
      assert total_records == records_fetch

  def signin(self):
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.chrome.options import Options

    from selenium.webdriver.chrome.options import Options
    chrome_options = Options()
    if not self.options.get('browser', False):
      chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")

    self.driver = webdriver.Chrome(options=chrome_options)
    self.driver.implicitly_wait(20)

    self.driver.get(('https://esaj.tjsp.jus.br/sajcas/login?service=https%3A%2F%2Fesaj.tjsp.jus.br%2Fesaj%2Fj_spring_cas_security_check'))
    WebDriverWait(self.driver, 15).until(EC.presence_of_element_located((By.ID, 'usernameForm')))

    username = os.getenv('TJSP_USERNAME')
    assert username
    password = os.getenv('TJSP_PASSWORD')
    assert password

    user_name = self.driver.find_element_by_id('usernameForm')
    user_name.send_keys(username)

    WebDriverWait(self.driver, 15) \
      .until(EC.presence_of_element_located((By.ID, 'passwordForm')))
    user_name = self.driver.find_element_by_id('passwordForm')
    user_name.send_keys(password)
    self.driver.find_element_by_id('pbEntrar').click()

    WebDriverWait(self.driver, 15) \
      .until(EC.presence_of_element_located((By.CLASS_NAME, 'esajTabelaServico')))

  def persist(self, record, **kwargs):
    self.output.save_from_contents(
      filepath=record['dest'],
      contents=record['source'],
      **kwargs)

  @utils.retryable(max_retries=9, sleeptime=10.)   # type: ignore
  def download_pdf(self, pdf):
    if pdf['url'] is None:
      return

    if self.output.exists(pdf['dest']):
      return

    if self.options.get('skip_pdf', False):
      return

    for c in self.request_cookies_browser:
      self.session.cookies.set(c['name'], c['value'])

    response = self.session.get(pdf['url'],
      headers=self.header_generator.generate(),
      allow_redirects=True,
      verify=False,
      timeout=10)

    if response.headers.get('Content-type') == 'application/pdf;charset=UTF-8':
      return response.content
    else:
      self.logger.warn(
        f"Got {response.status_code} when fetching {pdf['url']}. Content-type: {response.headers.get('Content-type')}.")
      raise PleaseRetryException()

  def chunks(self):
    import math
    ranges = list(utils.timely(
      self.params['start_date'], self.params['end_date'], unit='days', step=1))
    for start_date, end_date in reversed(ranges):
      number_of_records = self._set_search(start_date, end_date)
      number_of_pages   = math.ceil(number_of_records / 20)

      for page in range(1, number_of_pages + 1):
        chunk_params = {
          'start_date': start_date.to_date_string(),
          'end_date'  : end_date.to_date_string(),
          'page'      : page,
        }
        yield utils.Chunk(params=chunk_params, output=self.output,
          rows_generator=self.rows(
            start_date=start_date,
            end_date=end_date,
            page=page,
            expects=number_of_records))

  def rows(self, start_date, end_date, page, expects):
    text = self._get_search_results(page=page)
    soup = utils.soup_by_content(text)

    # Check whether the page matches the expected count of elements and page number.
    count_elements = soup.find_all('input', {'id': 'totalResultadoAbaRetornoFiltro-A'})
    assert len(count_elements) == 1
    records = int(count_elements[0]['value'])
    assert expects == records

    current_page_links = soup.find_all('span', {'class': 'paginaAtual'})
    if len(current_page_links) == 1:  # otherwise might be the last page.
      assert page == int(current_page_links[0].get_text().strip())

    # Firstly, get rows that matters
    items = soup.find_all('tr', {'class': 'fundocinza1'})

    for item in items:
      links = item.find_all('a', {'class': 'downloadEmenta'})
      assert len(links) > 0
      doc_id = links[0]['cdacordao']
      foro   = links[0]['cdforo']
      assert doc_id and foro

      # Will parse and store key values on this dict
      kvs = {}
      kvs['registro'] = links[0].get_text().strip()

      ementa_sem_formatacao_el = item.find_all('div', {'class': 'mensagemSemFormatacao'})
      assert len(ementa_sem_formatacao_el) == 1
      ementa_sem_formatacao = ementa_sem_formatacao_el[0].get_text()
      kvs['ementa_sem_formatacao'] = ementa_sem_formatacao

      fields_el = item.find_all('tr', {'class': 'ementaClass2'})
      for field in fields_el:
        # Clear label & values as we built a dict of them
        label = field.find_all('strong')[0].get_text().strip()
        label = re.sub(r'$:', '', label)

        # NOTE: Should we replace `<em>` to something else first?
        value = re.sub(r'\s+', ' ', field.get_text()).strip()
        value = value.replace(label, '')
        kvs[slugify(label)] = value.strip()

      _, month, year = kvs['data-de-publicacao'].split('/')

      #
      pdf_url = f'http://esaj.tjsp.jus.br/cjsg/getArquivo.do?cdAcordao={doc_id}&cdForo={foro}'

      yield {
          'source': item.prettify(encoding='cp1252'),
          'dest'  : f'{year}/{month}/doc_{doc_id}.html'
        }, {
          'source': json.dumps(kvs),
          'dest'  : f'{year}/{month}/doc_{doc_id}.json'
        }, {
          'url'   : pdf_url,
          'dest'  : f'{year}/{month}/doc_{doc_id}.pdf'
        }

  @utils.retryable(max_retries=9)  # type: ignore
  def _get_search_results(self, page):
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC

    url = f'https://esaj.tjsp.jus.br/cjsg/trocaDePagina.do?tipoDeDecisao=A&pagina={page}&conversationId='
    self.driver.get(url)
    WebDriverWait(self.driver, 15) \
      .until(EC.presence_of_element_located((By.ID, 'totalResultadoAbaRetornoFiltro-A')))
    return self.driver.page_source

  @utils.retryable(max_retries=9)  # type: ignore
  def _set_search(self, start_date, end_date):
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC

    start_ = '{day}/{month}/{year}'.format(day=start_date.day, month=start_date.month, year=start_date.year)
    end_   = '{day}/{month}/{year}'.format(day=end_date.day  , month=end_date.month, year=end_date.year)

    self.driver.get('https://esaj.tjsp.jus.br/cjsg/consultaCompleta.do')
    WebDriverWait(self.driver, 15) \
      .until(EC.presence_of_element_located((By.ID, 'iddados.buscaInteiroTeor')))

    search_box = self.driver.find_element_by_id('iddados.buscaInteiroTeor')
    search_box.send_keys('a ou de ou o')
    start_box = self.driver.find_element_by_id('iddados.dtPublicacaoInicio')
    start_box.send_keys(start_)
    end_box = self.driver.find_element_by_id('iddados.dtPublicacaoFim')
    end_box.send_keys(end_)
    WebDriverWait(self.driver, 15) \
      .until(EC.presence_of_element_located((By.ID, 'pbSubmit')))

    search_button = self.driver.find_element_by_id('pbSubmit')
    search_button.click()

    WebDriverWait(self.driver, 15).until(EC.presence_of_element_located((By.ID, 'totalResultadoAba-A')))

    # Refresh cookies -- they might rotate or do something else...
    self.request_cookies_browser = self.driver.get_cookies()

    # Get number of elements --  will use to validate page changes
    elements = utils.soup_by_content(self.driver.page_source) \
      .find_all('input', {'id': 'totalResultadoAba-A'})
    assert len(elements) == 1
    records = elements[0]['value']
    return int(records)

  @utils.retryable(max_retries=9)  # type: ignore
  def count(self):
    ranges = list(utils.timely(
      self.params['start_date'], self.params['end_date'], unit='months', step=1))
    total = 0
    for start_date, end_date in ranges:
      total += self._set_search(start_date, end_date)
    return total


@celery.task(queue='crawlers', default_retry_delay=5 * 60,
             autoretry_for=(BaseException,))
def tjsp_task(start_date, end_date, output_uri, pdf_async, skip_pdf, browser):
  start_date, end_date =\
    pendulum.parse(start_date), pendulum.parse(end_date)

  output = utils.get_output_strategy_by_path(path=output_uri)
  logger = logger_factory('tjsp')
  logger.info(f'Output: {output}.')

  crawler = tjsp(params={
    'start_date': start_date, 'end_date': end_date
  }, output=output, logger=logger, pdf_async=pdf_async, skip_pdf=skip_pdf, browser=browser)
  crawler.run()


@cli.command(name='tjsp')
@click.option('--start-date', prompt=True,   help='Format YYYY-MM-DD.')
@click.option('--end-date'  , prompt=True,   help='Format YYYY-MM-DD.')
@click.option('--output-uri', default=None,  help='Output URI (e.g. gs://bucket_name')
@click.option('--pdf-async' , default=False, help='Download PDFs async'   , is_flag=True)
@click.option('--skip-pdf'  , default=False, help='Skip PDF download'     , is_flag=True)
@click.option('--enqueue'   , default=False, help='Enqueue for a worker'  , is_flag=True)
@click.option('--browser'   , default=False, help='Open browser'          , is_flag=True)
def tjsp_command(start_date, end_date, output_uri, pdf_async, skip_pdf, enqueue, browser):
  args = (start_date, end_date, output_uri, pdf_async, skip_pdf, browser)
  if enqueue:
    tjsp_task.delay(*args)
  else:
    tjsp_task(*args)
