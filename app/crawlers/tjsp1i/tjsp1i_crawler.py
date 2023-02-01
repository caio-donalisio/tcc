import json
import math
import os
import re

from app.crawlers import base, utils, browsers
import pendulum
import requests
from celery_singleton import Singleton
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import WebDriverException
from urllib3.exceptions import InsecureRequestWarning
from app.crawlers.utils import PleaseRetryException
from app.crawlers.tjsp1i.tjsp1i_pdf import TJSP1IDownloader

from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

import click
import ratelimit
from app.crawler_cli import cli
from app.celery_run import celery_app as celery
from app.crawlers.logconfig import logger_factory, setup_cloud_logger

ONE_MINUTE = 60

logger = logger_factory('tjsp1i')


class TJSP1I(base.ICollector):
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
    use_cache = self.options.get('skip_cache', False) == False
    skip_pdf  = self.options.get('skip_pdf', False)

    cache_repository = cache_store = None
    if use_cache:
      cache_repository = base.HashedKeyValueRepository(output=self.output, prefix='.cache')
      cache_store      = base.HashedKeyValue(keys={
        'start_date': self.params['start_date'], 'end_date': self.params['end_date']})
      if cache_repository.exists(cache_store):
        cache_repository.restore(cache_store)

    for start_date, end_date in reversed(ranges):
      cache_key = base.HashedKeyValue(keys={  # just to compute the hash
        'start_date': start_date.to_date_string(),
        'end_date'  : end_date.to_date_string()
      })

      if cache_store and \
        cache_key.hash in cache_store.state:
        number_of_records = cache_store.state[cache_key.hash]['number_of_records']
        number_of_pages   = cache_store.state[cache_key.hash]['number_of_pages']
      else:
        number_of_records = self.client.set_search(start_date, end_date)
        number_of_pages   = math.ceil(number_of_records / 20)

        if cache_store:
          cache_store.set_value(cache_key.hash, {
            'number_of_records': number_of_records,
            'number_of_pages'  : number_of_pages,
            'start_date'       : start_date.to_date_string(),
            'end_date'         : end_date.to_date_string()
          })
          cache_repository.commit(cache_store)

      for page in range(1, number_of_pages + 3):
        chunk_params = {
          'start_date': start_date.to_date_string(),
          'end_date'  : end_date.to_date_string(),
          'page'      : page,
        }
        yield TJSP1IChunk(
          keys=chunk_params,
          prefix=f'{start_date.year}/{start_date.month:02d}/',
          client=self.client,
          start_date=start_date,
          end_date=end_date,
          skip_pdf=skip_pdf,
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


class TJSP1IClientPlain:

  def __init__(self, **options):
    self.header_generator = utils.HeaderGenerator(
      origin='http://esaj.tjsp.jus.br', xhr=False)
    self.session = requests.Session()
    self.headers = self.header_generator.generate()

  def signin(self):
    username = os.getenv('TJSP_USERNAME')
    password = os.getenv('TJSP_PASSWORD')
    assert username and password
    response =\
      self.session.post('http://esaj.tjsp.jus.br/sajcas/login?service=https%3A%2F%2Fesaj.tjsp.jus.br%2Fesaj%2Fj_spring_cas_security_check',
        headers=self.headers,
        allow_redirects=True,
        verify=False,
        data={
          'username': username,
          'password': password
        })
    return response

  @utils.retryable(max_retries=9)  # type: ignore
  def set_search(self, start_date, end_date):
    start_ = '{day}/{month}/{year}'.format(day=start_date.day, month=start_date.month, year=start_date.year)
    end_   = '{day}/{month}/{year}'.format(day=end_date.day  , month=end_date.month, year=end_date.year)

    response = self.session.post('http://esaj.tjsp.jus.br/cjsg/pesquisar.do',#http://esaj.tjsp.jus.br/cjsg/resultadoCompleta.do',
      headers=self.headers,
      verify=False,
      data={
        'conversationId': '',
        'dados.buscaInteiroTeor': '',
        'dados.pesquisarComSinonimos': 'S',
        'dados.buscaEmenta': '',
        'dados.nuProcOrigem': '',
        'dados.nuRegistro': '',
        'agenteSelectedEntitiesList': '',
        'contadoragente':  0,
        'contadorMaioragente':  0,
        'codigoCr': '',
        'codigoTr': '',
        'nmAgente': '',
        'juizProlatorSelectedEntitiesList': '',
        'contadorjuizProlator':  0,
        'contadorMaiorjuizProlator':  0,
        'codigoJuizCr': '',
        'codigoJuizTr': '',
        'nmJuiz': '',
        'classesTreeSelection.values': '',
        'classesTreeSelection.text': '',
        'assuntosTreeSelection.values': '',
        'assuntosTreeSelection.text': '',
        'comarcaSelectedEntitiesList': '',
        'contadorcomarca':  0,
        'contadorMaiorcomarca':  0,
        'cdComarca': '',
        'nmComarca': '',
        'secoesTreeSelection.values': '',
        'secoesTreeSelection.text': '',
        'dados.dtJulgamentoInicio': '',
        'dados.dtJulgamentoFim': '',
        'dados.dtPublicacaoInicio': start_,
        'dados.dtPublicacaoFim': end_,
        'dados.origensSelecionadas': 'T',
        'tipoDecisaoSelecionados':  'A',
        'dados.ordenarPor': 'dtPublicacao'
      })
    assert response.status_code == 200

    elements = utils.soup_by_content(response.text) \
      .find_all('input', {'id': 'totalResultadoAba-A'})
    assert len(elements) == 1
    records = elements[0]['value']
    return int(records)

  @utils.retryable(max_retries=9)  # type: ignore
  def get_search_results(self, page):
    # url = f'http://esaj.tjsp.jus.br/cjsg/trocaDePagina.do?tipoDeDecisao=A&pagina={page}&conversationId='
    url = f'http://esaj.tjsp.jus.br/cjsg/pesquisar.do?tipoDeDecisao=A&pagina={page}&conversationId='
    response = self.session.get(
      url,
      verify=False,
      headers=self.headers)
    return response.text

  def close(self):
    pass


class TJSP1IClient:

  def __init__(self, **options):
    self.header_generator = utils.HeaderGenerator(
      origin='https://esaj.tjsp.jus.br', xhr=False)
    self.session = requests.Session()
    self.request_cookies_browser = []
    self.options = (options or {})

    env = os.getenv('ENV', 'development')

    if env == "development":
      chrome_options = Options()

      if not self.options.get('browser', False):
        chrome_options.add_argument("--headless")

      chrome_options.add_argument("--no-sandbox")

      self.driver = browsers.FirefoxBrowser(headless=True).driver # webdriver. Chrome(options=chrome_options)
      self.driver.implicitly_wait(20)
    else:
      self.driver = webdriver.Remote(
        command_executor=os.getenv('SELENIUM_HUB_URI', 'http://selenium-hub:4444/wd/hub'),
        desired_capabilities=DesiredCapabilities.CHROME
      )

  def signin(self):
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait

    self.driver.get(('https://esaj.tjsp.jus.br/sajcas/login?service=https%3A%2F%2Fesaj.tjsp.jus.br%2Fesaj%2Fj_spring_cas_security_check'))
    WebDriverWait(self.driver, 15).until(EC.presence_of_element_located((By.ID, 'usernameForm')))

    username = os.getenv('TJSP_USERNAME')
    assert username
    password = os.getenv('TJSP_PASSWORD')
    assert password

    user_name = self.driver.find_element(By.ID, 'usernameForm')
    user_name.send_keys(username)

    WebDriverWait(self.driver, 15) \
      .until(EC.presence_of_element_located((By.ID, 'passwordForm')))
    user_name = self.driver.find_element(By.ID, 'passwordForm')
    user_name.send_keys(password)
    self.driver.find_element(By.ID, 'pbEntrar').click()

    WebDriverWait(self.driver, 15) \
      .until(EC.presence_of_element_located((By.CLASS_NAME, 'esajTabelaServico')))

    self.request_cookies_browser = self.driver.get_cookies()

  @utils.retryable(max_retries=9)  # type: ignore
  def get_search_results(self, page):
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC

    url = f'https://esaj.tjsp.jus.br/cjpg/trocarDePagina.do?pagina={page}&conversationId='
    self.driver.get(url)
    WebDriverWait(self.driver, 15) \
      .until(EC.presence_of_element_located((By.ID, 'divDadosResultado')))
    return self.driver.page_source

  @utils.retryable(max_retries=9)  # type: ignore
  def set_search(self, start_date, end_date):
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC

    start_ = '{day}/{month}/{year}'.format(day=start_date.day, month=start_date.month, year=start_date.year)
    end_   = '{day}/{month}/{year}'.format(day=end_date.day  , month=end_date.month, year=end_date.year)

    self.driver.get('https://esaj.tjsp.jus.br/cjpg/pesquisar.do?')
    WebDriverWait(self.driver, 15) \
      .until(EC.presence_of_element_located((By.ID, 'iddadosConsulta.pesquisaLivre')))

    start_box = self.driver.find_element(By.ID, 'iddadosConsulta.dtInicio')
    start_box.send_keys(start_)
    end_box = self.driver.find_element(By.ID, 'iddadosConsulta.dtFim')
    end_box.send_keys(end_)
    WebDriverWait(self.driver, 15) \
      .until(EC.presence_of_element_located((By.ID, 'pbSubmit')))

    search_button = self.driver.find_element(By.ID, 'pbSubmit')
    search_button.click()

    WebDriverWait(self.driver, 15).until(EC.presence_of_element_located((By.ID, 'resultados')))

    # Refresh cookies -- they might rotate or do something else...
    self.request_cookies_browser = self.driver.get_cookies()

    # Get number of elements --  will use to validate page changes
    elements = utils.soup_by_content(self.driver.page_source) \
      .find_all('td', {'bgcolor': '#EEEEEE'})
    elements = [tag for tag in elements if 'Resultados' in tag.text]
    assert len(elements) == 2
    records = int(re.search(r'.*de (\d+).*',elements[0].text).group(1))
    return int(records)

  def close(self):
    if self.driver:
      self.driver.quit()


class TJSP1IChunk(base.Chunk):

  def __init__(self, keys, prefix, client, start_date, end_date, skip_pdf, page, expects):
    super(TJSP1IChunk, self).__init__(keys, prefix)
    self.client     = client
    self.start_date = start_date
    self.end_date   = end_date
    self.skip_pdf   = skip_pdf
    self.page       = page
    self.expects    = expects

  def rows(self):
    rows = self.get_row_for_current_page()
    if rows:
      return rows
    else:
      return []

  @utils.retryable(max_retries=10., sleeptime=.5, ignore_if_exceeds=True, retryable_exceptions=(
  utils.PleaseRetryException,WebDriverException))
  def get_row_for_current_page(self):
    
    
    rows = []

    self.client.set_search(self.start_date, self.end_date)

    text = self.client.get_search_results(page=self.page)
    soup = utils.soup_by_content(text)

    # Check whether the page matches the expected count of elements and page number.
    count_elements = utils.soup_by_content(self.client.driver.page_source) \
      .find_all('td', {'bgcolor': '#EEEEEE'})
    count_elements = [tag for tag in count_elements if 'Resultados' in tag.text]
    assert len(count_elements) == 2
    records = int(re.search(r'.*de (\d+).*',count_elements[0].text).group(1))
    
    margin = 100. * abs(1 - (records / self.expects)) if self.expects > 0 else 0
    if margin >= 1.:
      logger.warn("page {page} was expecting {expects} got {records} (considers a margin of error) (start_date: {start_date}, end_date: {end_date})".format(
        page=self.page, expects=self.expects, records=records, start_date=self.start_date, end_date=self.end_date))
      raise utils.PleaseRetryException()

    current_page_links = soup.find_all('span', {'class': 'paginaAtual'})

    # Verify if we are at the right page... (at least when possible)
    if len(current_page_links) == 1:
      assert self.page == int(current_page_links[0].get_text().strip())

    is_last_page = len(soup.find_all('a', {'title': 'Próxima página'})) == 0

    # Firstly, get rows that matters
    items = soup.find_all('tr', {'class': 'fundocinza1'})
    
    if not is_last_page:
      page_records = len(items)
      if page_records == 0:  # Wasn't expecting that but sometimes it happens. Sadly
        raise utils.PleaseRetryException()

    for item in items:
      date = item.find(text=re.compile('.*Data de Disponibilização.*')).next
      day, month, year = date.strip().split('/')

      links = item.find_all('a', {'title': 'Visualizar Inteiro Teor'})
      assert len(links) == 1, f"Found {len(links)} links, expected 1."

      num_processo = item.find('span', class_='fonteNegrito').text.strip()
      kvs = {k:v for k,v in zip(['cdProcesso', 'cdForo', 'nmAlias', 'cdDoc'],links[0]['name'].split('-'))}
      assert f'{kvs["cdForo"]}{kvs["cdDoc"]}'.isdigit() and kvs["cdProcesso"] and kvs["nmAlias"]
      doc_id = f"{day}-{links[0]['name']}-{utils.extract_digits(num_processo)}"

      alt_meta_url = f"https://esaj.tjsp.jus.br/cpopg/show.do?processo.codigo={kvs['cdProcesso']}&processo.foro={kvs['cdForo']}&processo.numero={num_processo}"
      
      rows.append([
        base.Content(
          content=item.prettify(encoding='cp1252'),
          dest=f'{year}/{month}/{doc_id}.html',
          content_type='text/html'
        ),
        base.ContentFromURL(
          src=alt_meta_url,
          dest=f'{year}/{month}/{doc_id}_alt.html',
          content_type='text/html'
        )])

      if not self.skip_pdf:
        with browsers.FirefoxBrowser(headless=True) as browser:
          rows.append(
          base.Content(
            content= TJSP1IDownloader('').download_files(browser, item),
            dest=f'{year}/{month}/{doc_id}.pdf',
            content_type='application/pdf'
          ))

    # self.browser.quit()
    return rows




class TJSP1IHandler(base.ContentHandler):

  def __init__(self, output, client, **options):
    super(TJSP1IHandler, self).__init__(output)
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
    # for cookie in self.client.request_cookies_browser:
    #   self.client.session.cookies.set(cookie['name'], cookie['value'])
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

    if response.status_code == 200:
      logger.debug(f'GET {content_from_url.src} OK')
      self.output.save_from_contents(
        filepath=content_from_url.dest,
        contents=response.content,
        content_type=content_from_url.content_type)
    else:
      logger.warn(
        f"Got {response.status_code} when fetching {content_from_url.src}. Content-type: {response.headers.get('Content-type')}.")
      raise PleaseRetryException()


@celery.task(name='crawlers.tjsp1i', default_retry_delay=5 * 60,
             autoretry_for=(BaseException,),
             base=Singleton)
def tjsp1i_task(start_date, end_date, output_uri, pdf_async, skip_pdf, skip_cache, browser):
  from app.crawlers.logutils import logging_context

  with logging_context(crawler='tjsp1i'):
    start_date, end_date =\
      pendulum.parse(start_date), pendulum.parse(end_date)

    output = utils.get_output_strategy_by_path(path=output_uri)

    logger.info(f'Output: {output}.')
    setup_cloud_logger(logger)

    query_params = {
      'start_date': start_date, 'end_date': end_date
    }

    client  = TJSP1IClient(browser=browser)
    handler = TJSP1IHandler(output=output, client=client)

    collector = TJSP1I(params=query_params, output=output, client=client, skip_cache=skip_cache, skip_pdf=skip_pdf)

    snapshot = base.Snapshot(keys=query_params)
    base.get_default_runner(
        collector=collector, output=output, handler=handler, logger=logger, max_workers=4) \
      .run(snapshot=snapshot)


@celery.task(name='crawlers.tjsp1i', default_retry_delay=5 * 60,
             autoretry_for=(BaseException,),
             base=Singleton)
def tjsp1i_task_download_from_prev_days(output_uri, max_prev_days=1):
  start_date = pendulum.now().subtract(days=1)
  end_date   = pendulum.now().subtract(days=max_prev_days)
  tjsp1i_task(start_date.to_date_string(),
            end_date.to_date_string(),
            output_uri,
            pdf_async=True,
            skip_pdf=True,
            skip_cache=True,
            browser=False,)


@cli.command(name='tjsp1i')
@click.option('--start-date',
  default=utils.DefaultDates.THREE_MONTHS_BACK.strftime("%Y-%m-%d"),
  help='Format YYYY-MM-DD.',
)
@click.option('--end-date'  ,
  default=utils.DefaultDates.NOW.strftime("%Y-%m-%d"),
  help='Format YYYY-MM-DD.',
)
@click.option('--output-uri', default=None,  help='Output URI (e.g. gs://bucket_name')
@click.option('--pdf-async' , default=False, help='Download PDFs async'   , is_flag=True)
@click.option('--skip-pdf'  , default=False, help='Skip PDF download'     , is_flag=True)
@click.option('--skip-cache', default=False, help='Skip any cache crawler does', is_flag=True)
@click.option('--enqueue'   , default=False, help='Enqueue for a worker'  , is_flag=True)
@click.option('--browser'   , default=False, help='Open browser'          , is_flag=True)
@click.option('--split-tasks',
  default=None, help='Split tasks based on time range (weeks, months, days, etc) (use with --enqueue)')
def tjsp1i_command(start_date, end_date, output_uri, pdf_async, skip_pdf, skip_cache, enqueue, browser, split_tasks):
  args = (start_date, end_date, output_uri, pdf_async, skip_pdf, skip_cache, browser)
  if split_tasks:
    start_date, end_date =\
      pendulum.parse(start_date), pendulum.parse(end_date)
    for start, end in reversed(list(utils.timely(start_date, end_date, unit=split_tasks, step=1))):
      if enqueue:
        task_id = tjsp1i_task.delay(
          start.to_date_string(),
          end.to_date_string(),
          output_uri, pdf_async, skip_pdf, skip_cache, browser)
        print(f"task {task_id} sent with params {start.to_date_string()} {end.to_date_string()}")
      else:
        print(f"running with params {start.to_date_string()} {end.to_date_string()}")
        tjsp1i_task(
          start.to_date_string(),
          end.to_date_string(),
          output_uri, pdf_async, skip_pdf, skip_cache, browser)
  else:
    if enqueue:
      tjsp1i_task.delay(*args)
    else:
      tjsp1i_task(*args)
