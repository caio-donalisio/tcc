from app.crawlers import base, utils, browsers, captcha
import pendulum

from app.crawlers.logconfig import logger_factory, setup_cloud_logger

import bs4
import click
from app.celery_run import celery_app as celery
from app.crawler_cli import cli
import re
import time

from selenium.common.exceptions import UnexpectedAlertPresentException, JavascriptException
from selenium.webdriver.common.by import By

DEBUG = False
SITE_KEY = '6Lf778wZAAAAAKo4YvpkhvjwsrXd53EoJOWsWjAY'  # k value of recaptcha, found inside page
WEBSITE_URL = 'http://sagror.prefeitura.sp.gov.br/ManterDecisoes/pesquisaDecisoesCMT.aspx'
CMTSP_DATE_FORMAT = 'DDMMYYYY'
CRAWLER_DATE_FORMAT = 'YYYY-MM-DD'
DATE_PATTERN = re.compile(r'\d{2}/\d{2}/\d{4}')
FILES_PER_PAGE = 10
PDF_URL = ''
CMTSP_SEARCH_LINK = ''
DEFAULT_HEADERS = {
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


logger = logger_factory('cmtsp')


class CMTSPClient:

  def __init__(self):
    from app.crawlers import browsers
    # self.browser = browsers.FirefoxBrowser()
    # self.browser = browsers.FirefoxBrowser(headless=not DEBUG)

  @utils.retryable(max_retries=9, sleeptime=20)
  def setup(self):
    self.browser = browsers.FirefoxBrowser(headless=True)
    self.browser.get(WEBSITE_URL)

  @property
  def page_searched(self):
    return bool(self.browser.bsoup().find(name='span', text=re.compile(r'^\d+$')))

  def get_current_page(self, retries=3):
    for _ in range(retries):
      counter = self.browser.bsoup().find(name='span', text=re.compile(r'^\d+$'))
      if counter:
        return int(counter.text)
      else:
        print('sleeping')
        time.sleep(3)
    else:
      logger.warn('Could not find current page number')
      raise utils.PleaseRetryException()

  def search_is_over(self, current_page):
    return bool(
        not self.browser.bsoup().find('a', attrs={'href': "__doPostBack('grdPesquisaDecisoes$ctl14$ctl11','')"}) and
        not self.browser.bsoup().find('a', text=f'{current_page + 1}')
    )

  @utils.retryable(max_retries=9, sleeptime=20)
  def make_search(self, filters, by='date'):
    self.browser.driver.implicitly_wait(10)

    # ACEITA COOKIES
    if self.browser.bsoup().find('prodamsp-componente-consentimento'):
      try:
        self.browser.driver.execute_script('''
            document.querySelector("prodamsp-componente-consentimento").shadowRoot.querySelector("input[class='cc__button__autorizacao--all']").click()''')
      except JavascriptException:
        pass
    # PREENCHE DADOS
    self.browser.driver.implicitly_wait(10)
    if by == 'date':
      self.browser.fill_in('txtDtInicio', pendulum.parse(filters.get('start_date')).format(CMTSP_DATE_FORMAT))
      self.browser.fill_in('txtDtFim', pendulum.parse(filters.get('end_date')).format(CMTSP_DATE_FORMAT))
    elif by == 'process':
      self.browser.fill_in("txtExpressao", filters['process'])
    else:
      raise Exception(f'Option "{by}" not available')
    # RECAPTCHA
    captcha.solve_recaptcha(self.browser, logger, SITE_KEY)

    # CLICK 'PESQUISAR'
    self.browser.driver.find_element(By.ID, 'btnPesquisar').click()
    self.browser.driver.implicitly_wait(10)

    try:
      return bool(self.browser.bsoup().find('td', text='Ementa'))
    except UnexpectedAlertPresentException:
      return False

  @utils.retryable(max_retries=9, sleeptime=20)
  def count(self, filters):
    # Count not available
    return 0

  @utils.retryable(max_retries=9, sleeptime=20)
  def fetch(self, filters, page=1):

    while not self.page_searched:
      self.setup()
      self.make_search(filters)

    rows = True
    while self.get_current_page() != page:
      self.browser.driver.implicitly_wait(20)
      if not self.page_searched or not rows:
        self.make_search(filters)
      self.browser.driver.implicitly_wait(20)
      if page != self.get_current_page():
        # Checks if target page and current page belong to same 10 page block
        if (page-1)//10 != (self.get_current_page()-1)//10:
          if page > self.get_current_page():
            self.browser.driver.execute(f"""__doPostBack('grdPesquisaDecisoes$ctl14$ctl11','')""")
          else:
            self.browser.driver.execute(f"""__doPostBack('grdPesquisaDecisoes$ctl14$ctl00','')""")
        else:
          self.browser.driver.find_element(By.XPATH, f'//a[text()="{page}"]').click()
    return self.browser.bsoup()

  def get_pdf_session_id(self, tr):
    self.browser.driver.find_element(By.ID, tr.a['id']).click()
    self.browser.driver.implicitly_wait(3)
    main_window, pop_up_window = self.browser.driver.window_handles
    self.browser.driver.switch_to_window(pop_up_window)
    self.browser.driver.implicitly_wait(10)
    if self.browser.bsoup().find('div', class_='g-recaptcha'):
      raise Exception('Captcha not expected')
    self.browser.driver.close()
    self.browser.driver.switch_to_window(main_window)
    session_id = self.browser.get_cookie('ASP.NET_SessionId')
    return session_id


class CMTSPCollector(base.ICollector):

  def __init__(self, client, filters):
    self.client = client
    self.filters = filters

  @utils.retryable(max_retries=9, sleeptime=20)
  def count(self):
    return self.client.count(self.filters)

  @utils.retryable(max_retries=9, sleeptime=20)
  def chunks(self):
    ranges = utils.timely(
        pendulum.parse(self.filters['start_date']),
        pendulum.parse(self.filters['end_date']),
        unit='days', step=1)

    for start_date, end_date in reversed(list(ranges)):
      keys =\
          {'start_date': start_date.to_date_string(),
           'end_date': end_date.add(days=1).to_date_string()}

      yield CMTSPChunk(
          keys=keys,
          client=self.client,
          filters=self.filters,
          prefix=f'{start_date.year}/{start_date.month:02d}/'
      )


class CMTSPChunk(base.Chunk):
  def __init__(self, keys, client, filters, prefix):
    super(CMTSPChunk, self).__init__(keys, prefix)
    self.client = client
    self.filters = filters

  @utils.retryable(max_retries=3)
  def rows(self):
    page = 1
    self.client.setup()
    success = self.client.make_search(self.keys)
    if success:
      trs = []
      while True:
        soup = self.client.fetch(self.filters, page)
        trs = self._get_page_trs(soup)

        for tr in trs:
          date = pendulum.parse(self.keys.get('start_date'))
          year, month, day = date.year, date.month, date.day
          ementa_hash = utils.get_content_hash(tr, [{'name': 'td'}])
          process_code = utils.extract_digits(tr.find('td').text)
          filepath = f"{year}/{month:02}/{day}_{process_code}_{ementa_hash}"
          yield self.fetch_act_meta(tr, filepath)
          time.sleep(0.1)
        if self.client.search_is_over(page):
          break
        page += 1
    self.client.browser.driver.quit()

  @utils.retryable(max_retries=3)
  def fetch_act_meta(self, tr, filepath):
    session_date = f"Data de Julgamento: {self.keys['start_date']}"
    assert pendulum.parse(self.keys['start_date']).add(days=1) == pendulum.parse(self.keys['end_date'])

    # Manually inserts session date
    new_tag = bs4.Tag(name="td")
    new_tag.append(session_date)
    tr.insert(3, new_tag)
    return [
        base.Content(content=tr.prettify(), dest=f"{filepath}.html",
                     content_type='text/html'),
    ]

  def _get_page_trs(self, soup):
    trs = soup.find_all('tr')
    return trs[1:len(trs)-1]


@celery.task(name='crawlers.cmtsp', default_retry_delay=5 * 60,
             autoretry_for=(BaseException,))
def cmtsp_task(**kwargs):
  from app.crawlers import utils
  setup_cloud_logger(logger)

  from app.crawlers.logutils import logging_context

  with logging_context(crawler='cmtsp'):
    output = utils.get_output_strategy_by_path(
        path=kwargs.get('output_uri'))
    logger.info(f'Output: {output}.')

    query_params = {
        'start_date': kwargs.get('start_date'),
        'end_date': kwargs.get('end_date'),
    }

    collector = CMTSPCollector(client=CMTSPClient(), filters=query_params)
    handler = base.ContentHandler(output=output)
    snapshot = base.Snapshot(keys=query_params)

    base.get_default_runner(
        collector=collector,
        output=output,
        handler=handler,
        logger=logger,
        max_workers=8) \
        .run(snapshot=snapshot)


@cli.command(name='cmtsp')
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
def cmtsp_command(**kwargs):
  if kwargs.get('enqueue'):
    del (kwargs['enqueue'])
    split_tasks = kwargs.get('split_tasks', None)
    del (kwargs['split_tasks'])
    utils.enqueue_tasks(cmtsp_task, split_tasks, **kwargs)
  else:
    cmtsp_task(**kwargs)
