import base
import time
import utils
import pendulum
import click
import requests

from app import cli, celery

from logconfig import logger_factory, setup_cloud_logger
logger = logger_factory('titsp')

def get_filters(start_date : pendulum.DateTime, end_date : pendulum.DateTime):
  return {'__EVENTTARGET': 'ctl00$ConteudoPagina$gdvEntidade',
        '__VIEWSTATEGENERATOR': '255F25E5',
        'ctl00$ConteudoPagina$txbFiltroDataInicial': start_date.format('DD/MM/YYYY'),
        'ctl00$ConteudoPagina$txbFiltroDataFinal': end_date.format('DD/MM/YYYY'),
        'ctl00$ConteudoPagina$ddlFiltroRecurso': '0',
        'ctl00$ConteudoPagina$txbFiltroAiim': '',
        'ctl00$ConteudoPagina$ddlFiltroDrt': '0',
        'ctl00$ConteudoPagina$txbFiltroProcesso': '',
        'ctl00$ConteudoPagina$txbFiltroAnoProcesso': '',
        'ctl00$ConteudoPagina$txbFiltroEmenta': ''}


class TITSPClient:
  def __init__(self):
    self.base_url =\
      'https://www.fazenda.sp.gov.br/vdtit/consultarvotos.aspx?instancia=2'
    self.requester = requests.Session()
    self.data      = {}
    self.cookies   = {}

  @utils.retryable(max_retries=3)
  def count(self, filters):
    return 0

  @utils.retryable(max_retries=3)
  def fetch(self, filters, page=0):
    data = {**self.data, **filters}
    data['__EVENTTARGET']   = 'ctl00$ConteudoPagina$gdvEntidade'
    data['__EVENTARGUMENT'] = f'Page${page}'

    if page == 1:
      data['ctl00$ConteudoPagina$btnConsultar'] = 'Consultar'

    response = self.requester.post(self.base_url, data=data, cookies=self.cookies)
    soup = utils.soup_by_content(response.content)

    self.data['__VIEWSTATE']       = soup.find(id='__VIEWSTATE')['value']
    self.data['__EVENTVALIDATION'] = soup.find(id='__EVENTVALIDATION')['value']

    time.sleep(1)
    return soup

  def _get_cookies(self):
    response = self.requester.get(self.base_url)
    soup = utils.soup_by_content(response.content)
    self.cookies = {}
    for cookie in response.cookies:
      if cookie.name == 'ASP.NET_SessionId':
        self.cookies['ASP.NET_SessionId'] = cookie.value

    self.data['__VIEWSTATE'] = soup.find(id='__VIEWSTATE')['value']
    self.data['__EVENTVALIDATION'] = soup.find(id='__EVENTVALIDATION')['value']
    return response


class TITSPCollector(base.ICollector):

  def __init__(self, client : TITSPClient, query : dict, **options):
    self.client  = client
    self.query   = query
    self.options = (options or {})

  def count(self) -> int:
    return self.client.count(get_filters(
      self.query['start_date'], self.query['end_date']))

  def chunks(self):
    ranges = list(utils.timely(
      self.query['start_date'], self.query['end_date'], unit='weeks', step=1))

    for start_date, end_date in reversed(ranges):
      keys =\
        {'start_date': start_date.to_date_string(),
         'end_date'  : end_date.to_date_string()}

      yield TITSPChunk(keys=keys,
        client=self.client,
        filters=get_filters(start_date, end_date),
        prefix=f'{start_date.year}/{start_date.month:02d}/')


class TITSPChunk(base.Chunk):

  def __init__(self, keys, client, filters, prefix):
    super(TITSPChunk, self).__init__(keys, prefix)
    self.client  = client
    self.filters = filters

  @utils.retryable(max_retries=3)
  def rows(self):
    page = 1

    self.client._get_cookies()

    while True:
      soup = self.client.fetch(self.filters, page)
      trs  = self._get_page_trs(soup)

      for tr in trs:
        yield self.fetch_act(tr)
        time.sleep(0.1)

      if f'Page${page+1}' in str(soup):
        page += 1
      else:
        break

  @utils.retryable(max_retries=3)
  def fetch_act(self, tr):
      tds = tr.find_all('td')
      publication_date = tds[0].text
      act_id = tds[3].text
      html_filepath = utils.get_filepath(
          date=publication_date, filename=act_id, extension='html')
      pdf_href = tds[7].a['href']
      return [
        base.Content(content=tr.prettify(), dest=html_filepath,
          content_type='text/html'),
        self.fetch_pdf(pdf_href, act_id, publication_date)
      ]

  @utils.retryable(max_retries=3)
  def fetch_pdf(self, pdf_href, act_id, publication_date):
      data = self.client.data
      data['__EVENTTARGET'] = utils.find_between(
          pdf_href, start='WebForm_PostBackOptions\("', end='"')
      data['__EVENTARGUMENT'] = ''
      pdf_response = self.client.requester.post(self.client.base_url, data=data)
      pdf_filepath = utils.get_filepath(
          date=publication_date, filename=act_id, extension='pdf')
      return base.Content(content=pdf_response.content, dest=pdf_filepath,
        content_type='application/pdf')

  def _get_page_trs(self, soup):
      table = soup.find(class_='TABELA')
      return table.find_all(lambda tag: 'linha_grid' in tag.get('class', [''])[0])


@celery.task(queue='crawlers.titsp', default_retry_delay=5 * 60,
             autoretry_for=(BaseException,))
def titsp_task(start_date, end_date, output_uri):
  setup_cloud_logger(logger)

  from logutils import logging_context

  with logging_context(crawler='titsp'):
    output = utils.get_output_strategy_by_path(path=output_uri)
    logger.info(f'Output: {output}.')

    start_date, end_date =\
      pendulum.parse(start_date), pendulum.parse(end_date)

    query_params = {'start_date': start_date, 'end_date': end_date}
    collector = TITSPCollector(client=TITSPClient(), query=query_params)
    handler   = base.ContentHandler(output=output)

    snapshot = base.Snapshot(keys=query_params)
    base.get_default_runner(
        collector=collector, output=output, handler=handler, logger=logger, max_workers=8) \
      .run(snapshot=snapshot)


@cli.command(name='titsp')
@click.option('--start-date',
  default=utils.DefaultDates.BEGINNING_OF_YEAR_OR_SIX_MONTHS_BACK.strftime("%Y-%m-%d"),
  help='Format YYYY-MM-DD.',
)
@click.option('--end-date'  ,
  default=utils.DefaultDates.NOW.strftime("%Y-%m-%d"),
  help='Format YYYY-MM-DD.',
)
@click.option('--output-uri', default=None,  help='Output URI (e.g. gs://bucket_name')
@click.option('--enqueue'   , default=False, help='Enqueue for a worker'  , is_flag=True)
@click.option('--split-tasks',
  default=None, help='Split tasks based on time range (weeks, months, days, etc) (use with --enqueue)')
def titsp_command(start_date, end_date, output_uri, enqueue, split_tasks):
  args = (start_date, end_date, output_uri)
  if enqueue:
    if split_tasks:
      start_date, end_date =\
        pendulum.parse(start_date), pendulum.parse(end_date)
      for start, end in reversed(list(utils.timely(start_date, end_date, unit=split_tasks, step=1))):
        task_id = titsp_task.delay(
          start.to_date_string(),
          end.to_date_string(),
          output_uri)
        print(f"task {task_id} sent with params {start.to_date_string()} {end.to_date_string()}")
    else:
      titsp_task.delay(*args)
  else:
    titsp_task(*args)
