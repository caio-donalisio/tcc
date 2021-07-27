import re
import os
import json
import math
import requests
import pendulum
import utils
import time
import random
from slugify import slugify
import base

from selenium import webdriver
from celery_singleton import Singleton

from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait

from urllib3.exceptions import InsecureRequestWarning

from utils import PleaseRetryException
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

import ratelimit

import click
from app import cli, celery
from logconfig import logger_factory, setup_cloud_logger

ONE_MINUTE = 60

logger = logger_factory('tjsp')


class TJSP(base.ICollector):
  def __init__(self, params, output, client, **options):
    self.params = params
    self.output = output
    self.client = client
    self.options = (options or {})

  def setup(self):
    self.client.signin()

  def teardown(self):
    if self.client:
      self.client.close()

  def chunks(self):
    ranges = list(utils.timely(
      self.params['start_date'], self.params['end_date'], unit='days', step=1))

    # Will store number of records+pages based on parameters
    # This will avoid hitting the site to figure out the number of pages.
    cache_repository = base.HashedKeyValueRepository(output=self.output, prefix='.cache')
    cache_store      = base.HashedKeyValue(keys={'page_info_cache': True})

    if cache_repository.exists(cache_store):
      cache_repository.restore(cache_store)

    for start_date, end_date in reversed(ranges):
      cache_key = base.HashedKeyValue(keys={  # just to compute the hash
        'start_date': start_date.to_date_string(),
        'end_date'  : end_date.to_date_string()
      })

      if cache_key.hash in cache_store.state:
        number_of_records = cache_store.state[cache_key.hash]['number_of_records']
        number_of_pages   = cache_store.state[cache_key.hash]['number_of_pages']
      else:
        number_of_records = self.client.set_search(start_date, end_date)
        number_of_pages   = math.ceil(number_of_records / 20)
        cache_store.set_value(cache_key.hash, {
          'number_of_records': number_of_records,
          'number_of_pages'  : number_of_pages,
          'start_date'       : start_date.to_date_string(),
          'end_date'         : end_date.to_date_string()
        })
        cache_repository.commit(cache_store)

      for page in range(1, number_of_pages + 1):
        chunk_params = {
          'start_date': start_date.to_date_string(),
          'end_date'  : end_date.to_date_string(),
          'page'      : page,
        }
        yield TJSPChunk(
          keys=chunk_params,
          prefix=f'{start_date.year}/{start_date.month:02d}/',
          client=self.client,
          start_date=start_date,
          end_date=end_date,
          page=page,
          expects=number_of_records)

  @utils.retryable(max_retries=9)  # type: ignore
  def count(self):
    ranges = list(utils.timely(
      self.params['start_date'], self.params['end_date'], unit='months', step=1))
    total = 0
    for start_date, end_date in ranges:
      total += self.client.set_search(start_date, end_date)
    return total


class TJSPClient:

  def __init__(self, **options):
    self.header_generator = utils.HeaderGenerator(
      origin='https://esaj.tjsp.jus.br', xhr=False)
    self.session = requests.Session()
    self.request_cookies_browser = []
    self.options = (options or {})

    chrome_options = Options()
    if not self.options.get('browser', False):
      chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    self.driver = webdriver.Chrome(options=chrome_options)
    self.driver.implicitly_wait(20)

  def signin(self):
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.chrome.options import Options

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

  @utils.retryable(max_retries=9)  # type: ignore
  def get_search_results(self, page):
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC

    url = f'https://esaj.tjsp.jus.br/cjsg/trocaDePagina.do?tipoDeDecisao=A&pagina={page}&conversationId='
    self.driver.get(url)
    WebDriverWait(self.driver, 15) \
      .until(EC.presence_of_element_located((By.ID, 'totalResultadoAbaRetornoFiltro-A')))
    return self.driver.page_source

  @utils.retryable(max_retries=9)  # type: ignore
  def set_search(self, start_date, end_date):
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC

    start_ = '{day}/{month}/{year}'.format(day=start_date.day, month=start_date.month, year=start_date.year)
    end_   = '{day}/{month}/{year}'.format(day=end_date.day  , month=end_date.month, year=end_date.year)

    self.driver.get('https://esaj.tjsp.jus.br/cjsg/consultaCompleta.do')
    WebDriverWait(self.driver, 15) \
      .until(EC.presence_of_element_located((By.ID, 'iddados.buscaInteiroTeor')))

    search_box = self.driver.find_element_by_id('iddados.buscaInteiroTeor')
    search_box.send_keys('a ou de ou o')
    start_box = self.driver.find_element_by_id('iddados.dtJulgamentoInicio')
    start_box.send_keys(start_)
    end_box = self.driver.find_element_by_id('iddados.dtJulgamentoFim')
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

  def close(self):
    if self.driver:
      self.driver.quit()


class TJSPChunk(base.Chunk):

  def __init__(self, keys, prefix, client, start_date, end_date, page, expects):
    super(TJSPChunk, self).__init__(keys, prefix)
    self.client     = client
    self.start_date = start_date
    self.end_date   = end_date
    self.page       = page
    self.expects    = expects

  def rows(self):
    self.client.set_search(self.start_date, self.end_date)

    text = self.client.get_search_results(page=self.page)
    soup = utils.soup_by_content(text)

    # Check whether the page matches the expected count of elements and page number.
    count_elements = soup.find_all('input', {'id': 'totalResultadoAbaRetornoFiltro-A'})
    assert len(count_elements) == 1
    records = int(count_elements[0]['value'])

    assert self.expects == records, "page {page} was expecting {expects} got {records} (start_date: {start_date}, end_date: {end_date})".format(
      page=self.page, expects=self.expects, records=records, start_date=self.start_date, end_date=self.end_date)

    current_page_links = soup.find_all('span', {'class': 'paginaAtual'})

    # Verify if we are at the right page... (at least when possible)
    if len(current_page_links) == 1:
      assert self.page == int(current_page_links[0].get_text().strip())

    is_last_page = len(soup.find_all('a', {'title': 'Próxima página'})) == 0

    # Firstly, get rows that matters
    items = soup.find_all('tr', {'class': 'fundocinza1'})
    if not is_last_page:
      page_records = len(items)
      assert 20 == page_records, \
        f'expecting 20 records on page {self.page}, got {page_records} (start_date: {self.start_date}, end_date: {self.end_date})'  # sanity check

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

      _, month, year = kvs['data-do-julgamento'].split('/')

      #
      pdf_url = f'http://esaj.tjsp.jus.br/cjsg/getArquivo.do?cdAcordao={doc_id}&cdForo={foro}'
      row_content = item.prettify(encoding='cp1252')

      yield [
        base.Content(
          content=row_content,
          dest=f'{year}/{month}/doc_{doc_id}.html',
          content_type='text/html'
        ),
        base.Content(
          content=json.dumps(kvs),
          dest=f'{year}/{month}/doc_{doc_id}.json',
          content_type='application/json'
        ),
        base.ContentFromURL(
          src=pdf_url,
          dest=f'{year}/{month}/doc_{doc_id}.pdf',
          content_type='application/pdf'
        )
      ]


class TJSPHandler(base.ContentHandler):

  def __init__(self, output, client, **options):
    super(TJSPHandler, self).__init__(output)
    self.client  = client
    self.options = (options or {})

  @utils.retryable(max_retries=9)
  def handle(self, event):
    if isinstance(event, base.ContentFromURL):
      self.download(event)
    else:
      super().handle(event)

  def download(self, content_from_url):
    if content_from_url.src is None:
      return
    if self.options.get('skip_pdf', False):
      return
    if self.output.exists(content_from_url.dest):
      return
    for cookie in self.client.request_cookies_browser:
      self.client.session.cookies.set(cookie['name'], cookie['value'])
    return self._download(content_from_url)

  @ratelimit.sleep_and_retry
  @ratelimit.limits(calls=30, period=ONE_MINUTE)
  @utils.cooldown(2)
  @utils.retryable(max_retries=9)
  def _download(self, content_from_url):
    logger.debug(f'GET {content_from_url.src}')

    response = self.client.session.get(content_from_url.src,
      headers=self.client.header_generator.generate(),
      allow_redirects=True,
      verify=False,
      timeout=10)

    if response.headers.get('Content-type') == 'application/pdf;charset=UTF-8':
      logger.debug(f'GET {content_from_url.src} OK')
      self.output.save_from_contents(
        filepath=content_from_url.dest,
        contents=response.content,
        content_type=content_from_url.content_type)
    else:
      logger.warn(
        f"Got {response.status_code} when fetching {content_from_url.src}. Content-type: {response.headers.get('Content-type')}.")
      raise PleaseRetryException()


@celery.task(queue='crawlers.tjsp', default_retry_delay=5 * 60,
             autoretry_for=(BaseException,),
             base=Singleton)
def tjsp_task(start_date, end_date, output_uri, pdf_async, skip_pdf, browser):
  from logutils import logging_context

  with logging_context(crawler='tjsp'):
    start_date, end_date =\
      pendulum.parse(start_date), pendulum.parse(end_date)

    output = utils.get_output_strategy_by_path(path=output_uri)

    logger.info(f'Output: {output}.')
    setup_cloud_logger(logger)

    query_params = {
      'start_date': start_date, 'end_date': end_date
    }

    client  = TJSPClient(browser=browser)
    handler = TJSPHandler(output=output, client=client, skip_pdf=skip_pdf)

    collector = TJSP(params=query_params, output=output, client=client)

    snapshot = base.Snapshot(keys=query_params)
    base.get_default_runner(
        collector=collector, output=output, handler=handler, logger=logger, max_workers=4) \
      .run(snapshot=snapshot)


@cli.command(name='tjsp')
@click.option('--start-date', prompt=True,   help='Format YYYY-MM-DD.')
@click.option('--end-date'  , prompt=True,   help='Format YYYY-MM-DD.')
@click.option('--output-uri', default=None,  help='Output URI (e.g. gs://bucket_name')
@click.option('--pdf-async' , default=False, help='Download PDFs async'   , is_flag=True)
@click.option('--skip-pdf'  , default=False, help='Skip PDF download'     , is_flag=True)
@click.option('--enqueue'   , default=False, help='Enqueue for a worker'  , is_flag=True)
@click.option('--browser'   , default=False, help='Open browser'          , is_flag=True)
@click.option('--split-tasks',
  default=None, help='Split tasks based on time range (weeks, months, days, etc) (use with --enqueue)')
def tjsp_command(start_date, end_date, output_uri, pdf_async, skip_pdf, enqueue, browser, split_tasks):
  args = (start_date, end_date, output_uri, pdf_async, skip_pdf, browser)
  if enqueue:
    if split_tasks:
      start_date, end_date =\
        pendulum.parse(start_date), pendulum.parse(end_date)
      for start, end in utils.timely(start_date, end_date, unit=split_tasks, step=1):
        task_id = tjsp_task.delay(
          start.to_date_string(),
          end.to_date_string(),
          output_uri, pdf_async, skip_pdf, browser)
        print(f"task {task_id} sent with params {start.to_date_string()} {end.to_date_string()}")
    else:
      tjsp_task.delay(*args)
  else:
    tjsp_task(*args)
