from app.crawlers.logconfig import logger_factory, setup_cloud_logger
from app.celery_run import celery_app as celery
from app.crawler_cli import cli
import ratelimit
import click
import json
import math
import os
import re

from app.crawlers import base, utils
import pendulum
import requests
from celery_singleton import Singleton
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from slugify import slugify
from urllib3.exceptions import InsecureRequestWarning
from app.crawlers.utils import PleaseRetryException

from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)


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
    use_cache = self.options.get('skip_cache', False) == False

    cache_repository = cache_store = None
    if use_cache:
      cache_repository = base.HashedKeyValueRepository(output=self.output, prefix='.cache')
      cache_store = base.HashedKeyValue(keys={
          'start_date': self.params['start_date'], 'end_date': self.params['end_date']})
      if cache_repository.exists(cache_store):
        cache_repository.restore(cache_store)

    for start_date, end_date in reversed(ranges):
      cache_key = base.HashedKeyValue(keys={  # just to compute the hash
          'start_date': start_date.to_date_string(),
          'end_date': end_date.to_date_string()
      })

      if cache_store and \
              cache_key.hash in cache_store.state:
        number_of_records = cache_store.state[cache_key.hash]['number_of_records']
        number_of_pages = cache_store.state[cache_key.hash]['number_of_pages']
      else:
        number_of_records = self.client.set_search(start_date, end_date)
        number_of_pages = math.ceil(number_of_records / 20)

        if cache_store:
          cache_store.set_value(cache_key.hash, {
              'number_of_records': number_of_records,
              'number_of_pages': number_of_pages,
              'start_date': start_date.to_date_string(),
              'end_date': end_date.to_date_string()
          })
          cache_repository.commit(cache_store)

      for page in range(1, number_of_pages + 3):
        chunk_params = {
            'start_date': start_date.to_date_string(),
            'end_date': end_date.to_date_string(),
            'page': page,
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


class TJSPClientPlain:

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
    end_ = '{day}/{month}/{year}'.format(day=end_date.day, month=end_date.month, year=end_date.year)

    response = self.session.post('http://esaj.tjsp.jus.br/cjsg/resultadoCompleta.do',
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
    url = f'http://esaj.tjsp.jus.br/cjsg/trocaDePagina.do?tipoDeDecisao=A&pagina={page}&conversationId='
    response = self.session.get(
        url,
        verify=False,
        headers=self.headers)
    return response.text

  def close(self):
    pass


class TJSPClient:

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

      self.driver = webdriver.Chrome(options=chrome_options)
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

    self.driver.get(
        ('https://esaj.tjsp.jus.br/sajcas/login?service=https%3A%2F%2Fesaj.tjsp.jus.br%2Fesaj%2Fj_spring_cas_security_check'))
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
    end_ = '{day}/{month}/{year}'.format(day=end_date.day, month=end_date.month, year=end_date.year)

    self.driver.get('https://esaj.tjsp.jus.br/cjsg/consultaCompleta.do')
    WebDriverWait(self.driver, 15) \
        .until(EC.presence_of_element_located((By.ID, 'iddados.buscaInteiroTeor')))

    search_box = self.driver.find_element(By.ID, 'iddados.buscaInteiroTeor')
    search_box.send_keys('a ou de ou o')
    start_box = self.driver.find_element(By.ID, 'iddados.dtJulgamentoInicio')
    start_box.send_keys(start_)
    end_box = self.driver.find_element(By.ID, 'iddados.dtJulgamentoFim')
    end_box.send_keys(end_)
    WebDriverWait(self.driver, 15) \
        .until(EC.presence_of_element_located((By.ID, 'pbSubmit')))

    search_button = self.driver.find_element(By.ID, 'pbSubmit')
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
    self.client = client
    self.start_date = start_date
    self.end_date = end_date
    self.page = page
    self.expects = expects

  def rows(self):
    rows = self.get_row_for_current_page()
    if rows:
      return rows
    else:
      return []

  @utils.retryable(max_retries=10., sleeptime=.5, ignore_if_exceeds=True, retryable_exceptions=(
      utils.PleaseRetryException,))
  def get_row_for_current_page(self):
    rows = []

    self.client.set_search(self.start_date, self.end_date)

    text = self.client.get_search_results(page=self.page)
    soup = utils.soup_by_content(text)

    # Check whether the page matches the expected count of elements and page number.
    count_elements = soup.find_all('input', {'id': 'totalResultadoAbaRetornoFiltro-A'})
    assert len(count_elements) == 1
    records = int(count_elements[0]['value'])

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
      links = item.find_all('a', {'class': 'downloadEmenta'})
      assert len(links) > 0
      doc_id = links[0]['cdacordao']
      foro = links[0]['cdforo']
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

      rows.append([
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
      ])

    return rows


class TJSPHandler(base.ContentHandler):

  def __init__(self, output, client, **options):
    super(TJSPHandler, self).__init__(output)
    self.client = client
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


@celery.task(name='crawlers.tjsp', default_retry_delay=5 * 60,
             autoretry_for=(BaseException,),
             base=Singleton)
def tjsp_task(start_date, end_date, output_uri, pdf_async, skip_pdf, skip_cache, browser):
  from app.crawlers.logutils import logging_context

  with logging_context(crawler='tjsp'):
    start_date, end_date =\
        pendulum.parse(start_date), pendulum.parse(end_date)

    output = utils.get_output_strategy_by_path(path=output_uri)

    logger.info(f'Output: {output}.')
    setup_cloud_logger(logger)

    query_params = {
        'start_date': start_date, 'end_date': end_date
    }

    client = TJSPClient(browser=browser)
    handler = TJSPHandler(output=output, client=client, skip_pdf=skip_pdf)

    collector = TJSP(params=query_params, output=output, client=client, skip_cache=skip_cache)

    snapshot = base.Snapshot(keys=query_params)
    base.get_default_runner(
        collector=collector, output=output, handler=handler, logger=logger, max_workers=4) \
        .run(snapshot=snapshot)


@celery.task(name='crawlers.tjsp', default_retry_delay=5 * 60,
             autoretry_for=(BaseException,),
             base=Singleton)
def tjsp_task_download_from_prev_days(output_uri, max_prev_days=1):
  start_date = pendulum.now().subtract(days=1)
  end_date = pendulum.now().subtract(days=max_prev_days)
  tjsp_task(start_date.to_date_string(),
            end_date.to_date_string(),
            output_uri,
            pdf_async=True,
            skip_pdf=True,
            skip_cache=True,
            browser=False,)


@cli.command(name='tjsp')
@click.option('--start-date',
              default=utils.DefaultDates.THREE_MONTHS_BACK.strftime("%Y-%m-%d"),
              help='Format YYYY-MM-DD.',
              )
@click.option('--end-date',
              default=utils.DefaultDates.NOW.strftime("%Y-%m-%d"),
              help='Format YYYY-MM-DD.',
              )
@click.option('--output-uri', default=None,  help='Output URI (e.g. gs://bucket_name')
@click.option('--pdf-async', default=False, help='Download PDFs async', is_flag=True)
@click.option('--skip-pdf', default=False, help='Skip PDF download', is_flag=True)
@click.option('--skip-cache', default=False, help='Skip any cache crawler does', is_flag=True)
@click.option('--enqueue', default=False, help='Enqueue for a worker', is_flag=True)
@click.option('--browser', default=False, help='Open browser', is_flag=True)
@click.option('--split-tasks',
              default=None, help='Split tasks based on time range (weeks, months, days, etc) (use with --enqueue)')
def tjsp_command(**kwargs):
  if kwargs.get('enqueue'):
    del (kwargs['enqueue'])
    split_tasks = kwargs.get('split_tasks', None)
    del (kwargs['split_tasks'])
    utils.enqueue_tasks(tjsp_task, split_tasks, **kwargs)
  else:
    tjsp_task(*kwargs)


@ cli.command(name='tjsp-validate')
@ click.option('--start-date',
               default=utils.DefaultDates.THREE_MONTHS_BACK.strftime("%Y-%m-%d"),
               help='Format YYYY-MM-DD.',
               )
@ click.option('--end-date',
               default=utils.DefaultDates.NOW.strftime("%Y-%m-%d"),
               help='Format YYYY-MM-DD.',
               )
@ click.option('--output-uri', default=None,  help='Output URI (e.g. gs://bucket_name')
@ click.option('--count-pending-pdfs', default=False, help='Count pending pdfs', is_flag=True)
def tjsp_validate(start_date, end_date, output_uri, count_pending_pdfs):
  from app.crawlers.tjsp.tjsp_utils import list_pending_pdfs
  from tabulate import tabulate
  from tqdm import tqdm

  start_date, end_date = pendulum.parse(start_date), pendulum.parse(end_date)

  # Read out all avaliable snapshots to assess about completeness of data
  output = utils.get_output_strategy_by_path(path=output_uri)
  repository = base.SnapshotsRepository(output=output)

  results = []

  for start, end in tqdm(list(utils.timely(start_date, end_date, unit='months', step=1))):
    query_params = {
        'start_date': start, 'end_date': end
    }
    snapshot = base.Snapshot(keys=query_params)

    if repository.exists(snapshot):
      repository.restore(snapshot)
      snapshot_info = {
          key: snapshot.get_value(key)
          for key in ['records', 'total_records', 'expects', 'done']
      }

      pending_pdfs_count = 0

      if snapshot_info['done'] == True:
        logger.debug(
            f'Snapshot {snapshot.hash} params {start.to_date_string()}-{end.to_date_string()} = {snapshot_info}.')

        # validate pdfs as well.
        if count_pending_pdfs:
          prefix = f'{start.year}/{start.month:02d}/'
          pending_pdfs =\
              list_pending_pdfs(bucket_name=output._bucket_name, prefix=prefix)
          pending_pdfs_count = len(list(pending_pdfs))
          logger.debug(f'Prefix: {prefix} - pending pdfs: {pending_pdfs_count}')

      total_records = (snapshot_info.get('total_records') or 0)
      expects = (snapshot_info.get('expects') or 0)

      results.append([
          start.to_date_string(),
          end.to_date_string(),
          expects,
          total_records,
          total_records - expects,
          pending_pdfs_count,
          snapshot_info['done'],
          snapshot.hash
      ])
    else:
      results.append([
          start.to_date_string(),
          end.to_date_string(),
          None,
          None,
          None,
          0,
          'Unknown',
          '-',
      ])

  total_expected = sum([row[2] for row in results if row[2]])
  total_records = sum([row[3] for row in results if row[3]])
  total_pdfs = sum([row[5] for row in results if row[5]])
  results.append(['', '', total_expected, total_records, total_records - total_expected, total_pdfs, ''])

  print(tabulate(results, headers=['Start', 'End', 'Expects',
        'Records', 'Diff', 'Pending PDFs', 'Finished', 'Snapshot']))
