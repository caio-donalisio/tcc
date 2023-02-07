from app.crawlers import base, browsers, utils, captcha
import math
import pendulum
from app.crawlers.logconfig import logger_factory, setup_cloud_logger
import click
from app.crawler_cli import cli
from app.celery_run import celery_app as celery
import re

DEBUG = False
SITE_KEY = '6LfkZ24UAAAAAMO1KEF_pP-G3wE0dYN69-SG8NxI'  # k value of recaptcha, found inside page
WEBSITE_URL = 'https://www2.cjf.jus.br/jurisprudencia/trf1/index.xhtml'
TRF1_DATE_FORMAT = 'DD/MM/YYYY'
CRAWLER_DATE_FORMAT = 'YYYY-MM-DD'
DATE_PATTERN = re.compile(r'\d{2}/\d{2}/\d{4}')
FILES_PER_PAGE = 30  # 10, 30 or 50
PDF_URL = 'https://pje2g.trf1.jus.br/consultapublica/ConsultaPublica/DetalheProcessoConsultaPublica/documentoSemLoginHTML.seam'
TRF1_SEARCH_LINK = 'https://pje2g.trf1.jus.br/consultapublica/ConsultaPublica/listView.seam'
DOC_TO_PDF_CONTAINER_URL = 'http://localhost/unoconv/pdf'

logger = logger_factory('trf1')


class TRF1Client:

  def __init__(self):
    self.browser = browsers.FirefoxBrowser(headless=not DEBUG)

  @utils.retryable(max_retries=9, sleeptime=20)
  def setup(self):
    self.browser.get(WEBSITE_URL)

  @property
  def page_searched(self):
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(self.browser.page_source(), 'html.parser')
    return bool(soup.find(name='span', attrs={'class': "ui-paginator-current"}))

  @utils.retryable(max_retries=9, sleeptime=20)
  def make_search(self, filters):
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait
    self.browser.wait_for_element((By.ID, "formulario:ckbAvancada")).click()

    # SELECT DATA DE PUBLICAÇÃO
    WebDriverWait(self.browser.driver, 10).until(
        EC.element_to_be_clickable((By.CLASS_NAME, 'ui-icon-triangle-1-s'))).click()
    WebDriverWait(self.browser.driver, 10).until(
        EC.element_to_be_clickable((By.ID, 'formulario:combo_tipo_data_1'))).click()

    # INSERT DATE RANGE
    WebDriverWait(self.browser.driver, 10).until(EC.element_to_be_clickable((By.ID, "formulario:j_idt37_input"))).send_keys(
        pendulum.parse(filters.get('start_date')).format(TRF1_DATE_FORMAT))
    WebDriverWait(self.browser.driver, 10).until(EC.element_to_be_clickable((By.ID, "formulario:j_idt39_input"))).send_keys(
        pendulum.parse(filters.get('end_date')).format(TRF1_DATE_FORMAT))

    captcha.solve_recaptcha(self.browser, logger, SITE_KEY)

    # CLICK 'PESQUISAR'
    self.browser.driver.find_element(By.ID, 'formulario:actPesquisar').click()
    self.browser.driver.implicitly_wait(10)

    # SELECT NUMBER OF PROCESS PER PAGE
    WebDriverWait(self.browser.driver, 10).until(
        EC.element_to_be_clickable((By.ID, 'formulario:tabelaDocumentos:j_id23')))
    self.browser.select_by_id(field_id='formulario:tabelaDocumentos:j_id23', option=FILES_PER_PAGE)

    self.browser.driver.implicitly_wait(10)

  @utils.retryable(max_retries=9, sleeptime=20)
  def count(self, filters):

    result = self.fetch(filters)
    div = result.find(id='formulario:j_idt61:0:j_idt65:0:ajax')
    count = int(utils.extract_digits(div.text))
    return count

  @utils.retryable(max_retries=9, sleeptime=20)
  def fetch(self, filters, page=1):
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.common.by import By
    import time

    def get_current_page():
      PAGE_PATTERN = r'.*Página: (\d+)\/.*'
      return int(re.search(
          PAGE_PATTERN, self.browser.bsoup().find('span', class_='ui-paginator-current').text).group(1)
      )

    while not self.page_searched:
      self.setup()
      self.make_search(filters)

    rows = True
    while get_current_page() != page:
      self.browser.driver.implicitly_wait(20)
      if not self.page_searched or not rows:
        self.make_search(filters)
      self.browser.driver.implicitly_wait(20)
      current_page = get_current_page()
      if current_page != page:
        to_click_class = 'ui-icon-seek-next' if current_page < page else 'ui-icon-seek-prev'
        WebDriverWait(self.browser.driver, 20).until(
            EC.element_to_be_clickable((By.CLASS_NAME, to_click_class))).click()
        time.sleep(3.5)
      self.browser.driver.implicitly_wait(20)
      rows = self.browser.bsoup().find_all(name='div', attrs={'class': "ui-datagrid-column ui-g-12 ui-md-12"})
    if not get_current_page() or not rows:
      raise utils.PleaseRetryException()
    return self.browser.bsoup()


class TRF1Collector(base.ICollector):

  def __init__(self, client, filters):
    self.client = client
    self.filters = filters

  @utils.retryable(max_retries=9, sleeptime=20)
  def count(self):
    return self.client.count(self.filters)

  @utils.retryable(max_retries=9, sleeptime=20)
  def chunks(self):
    total = self.count()
    pages = math.ceil(total/FILES_PER_PAGE)

    for page in range(1, pages + 1):
      yield TRF1Chunk(
          keys={**self.filters, **{'page': page, 'total': total}},
          prefix='',
          page=page,
          total=total,
          filters=self.filters,
          client=self.client,
      )


class TRF1Chunk(base.Chunk):

  def __init__(self, keys, prefix, page, total, filters, client):
    super(TRF1Chunk, self).__init__(keys, prefix)
    self.page = page
    self.total = total
    self.filters = filters
    self.client = client

  @utils.retryable(max_retries=9, sleeptime=20)
  def rows(self):
    # REFACTOR
    from app.crawlers.trf1 import trf1_pdf

    DATE_PATTERN = r'Data da publicação[^\d]*(?P<date>(?P<day>\d{2})\/(?P<month>\d{2})\/(?P<year>\d{4}))'
    page_soup = self.client.fetch(self.filters, page=self.page)
    current_page = int(re.search(r'.*Página: (\d+)\/.*', page_soup.find('span',
                       class_='ui-paginator-current').text).group(1))
    assert current_page == self.page
    rows = page_soup.find_all(name='div', attrs={'class': "ui-datagrid-column ui-g-12 ui-md-12"})
    for n, row in enumerate(rows, 1):
      pdf_content, inteiro_page_content = trf1_pdf.TRF1Downloader(
          '').download_files(row) if not self.filters.get('skip_pdf') else '', ''
      meta_hash = utils.get_content_hash(row, [{'name': 'td'}])
      acordao_titulo = row.find(attrs={'class': "titulo_doc"}).text
      process_number = utils.extract_digits(acordao_titulo)
      pub_date = re.search(DATE_PATTERN, row.text)
      to_download = []

      base_dir = f"{pub_date.groupdict()['year']}/{pub_date.groupdict()['month']}"
      # _{utils.get_pdf_hash(pdf_content)}"
      filename = f"{pub_date.groupdict().get('day')}_{process_number}_{meta_hash}"
      base_path = f"{base_dir}/{filename}"

      to_download.append(
          base.Content(content=str(row),
                       dest=f"{base_path}_A.html",
                       content_type='text/html'))

      if inteiro_page_content:
        to_download.append(
            base.Content(content=inteiro_page_content,
                         dest=f"{base_path}_B.html",
                         content_type='text/html'))

      if pdf_content:
        to_download.append(
            base.Content(content=pdf_content,
                         dest=f"{base_path}.pdf",
                         content_type='application/pdf'))

      yield to_download


@celery.task(name='crawlers.trf1', default_retry_delay=5 * 60,
             autoretry_for=(BaseException,))
def trf1_task(**kwargs):
  setup_cloud_logger(logger)

  from app.crawlers.logutils import logging_context

  with logging_context(crawler='trf1'):
    output = utils.get_output_strategy_by_path(
        path=kwargs.get('output_uri'))
    logger.info(f'Output: {output}.')

    query_params = {
        'start_date': kwargs.get('start_date'),
        'end_date': kwargs.get('end_date'),
        'skip_pdf': kwargs.get('skip_pdf'),
    }

    collector = TRF1Collector(client=TRF1Client(), filters=query_params)
    handler = base.ContentHandler(output=output)
    snapshot = base.Snapshot(keys=query_params)

    base.get_default_runner(
        collector=collector,
        output=output,
        handler=handler,
        logger=logger,
        max_workers=8) \
        .run(snapshot=snapshot)


@cli.command(name='trf1')
@click.option('--start-date',
              default=utils.DefaultDates.THREE_MONTHS_BACK.strftime("%Y-%m-%d"),
              help='Format YYYY-MM-DD.',
              )
@click.option('--end-date',
              default=utils.DefaultDates.NOW.strftime("%Y-%m-%d"),
              help='Format YYYY-MM-DD.',
              )
@click.option('--output-uri',    default=None,     help='Output URI (e.g. gs://bucket_name')
@click.option('--enqueue',    default=False,    help='Enqueue for a worker', is_flag=True)
@click.option('--split-tasks',
              default=None, help='Split tasks based on time range (weeks, months, days, etc) (use with --enqueue)')
@click.option('--skip-pdf', default=False, help='Skips pdf collection, collects only metadata', is_flag=True)
def trf1_command(**kwargs):
  if kwargs.get('enqueue'):
    del (kwargs['enqueue'])
    split_tasks = kwargs.get('split_tasks', None)
    del (kwargs['split_tasks'])
    utils.enqueue_tasks(trf1_task, split_tasks, **kwargs)
  else:
    trf1_task(*kwargs)
