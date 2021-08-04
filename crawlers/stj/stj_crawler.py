import re
import base
import utils
import pendulum
import click
import requests

from app import cli, celery

from logconfig import logger_factory, setup_cloud_logger
logger = logger_factory('stj')


def get_filters(start_date : pendulum.DateTime, end_date : pendulum.DateTime):
  date_filter = f'@DTPB >= {start_date.format("YYYYMMDD")} E @DTPB <= {end_date.format("YYYYMMDD")}'
  return {
    'acao': 'pesquisar',
    'novaConsulta': 'true',
    'b': 'ACOR',
    'data': date_filter,
    'operador': 'e',
    'thesaurus': 'JURIDICO',
    'p': 'true',
    'processo': '',
    'relator': '',
    'data_inicial': start_date,
    'data_final': end_date,
    'tipo_data': 'DTPB',
    'g-recaptcha-response': ''
  }


class STJClient:

  def __init__(self):
    self.base_url  = 'https://scon.stj.jus.br'
    self.requester = requests.Session()

  def reset_session(self):
    self.requester = requests.Session()

  @utils.retryable(max_retries=3)
  def count(self, filters):
    response = self._response_or_retry(
      self.fetch(filters, offset=0))
    return self._count_by_content(response.content)

  @utils.retryable(max_retries=3)
  def fetch(self, filters, offset):
    return self._response_or_retry(self.requester.post(
      f'{self.base_url}/SCON/pesquisar.jsp',
      headers={'ContentType': 'application/X-www-form-urlencoded'},
      data={**filters, 'i': offset}))

  @utils.retryable(max_retries=3)
  def get(self, path):
    return self.requester.get(f'{self.base_url}/{path}')

  def _response_or_retry(self, response):
    soup = utils.soup_by_content(response.content)

    if soup \
        .find('div', id='idCaptchaLinha'):
      logger.warn('Got captcha -- reseting session.')
      self.reset_session()
      raise utils.PleaseRetryException()

    if soup \
        .find('div', {'id': 'infopesquisa'}) is None:
      logger.warn('Got something else -- reseting session and retrying.')
      self.reset_session()
      raise utils.PleaseRetryException()

    return response

  def _count_by_content(self, content):
    soup = utils.soup_by_content(content)
    info = soup.find('div', {'id': 'infopesquisa'})
    if not info:
      assert info is not None
    div_count = info.find_all('div', {'class': 'divCell'})[0]
    match = re.match(r'(\d+\.?\d+)', div_count.get_text())
    if match:
      return int(match.group(0).replace('.', ''))
    else:
      return 0


class STJCollector(base.ICollector):

  def __init__(self, client : STJClient, query : dict, **options):
    self.client  = client
    self.query   = query
    self.options = (options or {})

  def count(self) -> int:
    return self.client.count(get_filters(
      self.query['start_date'], self.query['end_date']))

  def chunks(self):
    ranges = list(utils.timely(
      self.query['start_date'], self.query['end_date'], unit='days', step=1))

    for start_date, end_date in reversed(ranges):
      filters = get_filters(start_date, end_date)
      count   = self.client.count(filters)

      keys =\
        {'start_date' : start_date.to_date_string(),
          'end_date'  : end_date.to_date_string(),
          'limit'     : count + 1}

      yield STJChunk(keys=keys,
        client=self.client,
        filters=filters,
        limit=count + 1,
        prefix=f'{start_date.year}/{start_date.month:02d}/')


class STJChunk(base.Chunk):

  def __init__(self, keys, client, filters, limit, prefix):
    super(STJChunk, self).__init__(keys, prefix)
    self.client  = client
    self.filters = filters
    self.limit   = limit

  @utils.retryable(max_retries=3)
  def rows(self):
    response = self.client.fetch({
      **self.filters, **{'l': self.limit, 'numDocsPagina': self.limit}}, offset=0)
    soup  = utils.soup_by_content(response.content)
    count = self.client._count_by_content(response.content)
    if count == 0:
      return

    for content in self.page_contents(soup):
      yield content

  def page_contents(self, soup):
    docs = soup.find_all(class_='documento')

    def _get_pdf_url(doc):
      pdf_link = doc.find('div', class_='iconesAcoes').a
      return utils.find_between(pdf_link['href'], start="'", end="'")

    for doc in docs:
      pdf_url = _get_pdf_url(doc)
      act_id  = utils.get_param_from_url(pdf_url, 'num_registro')
      publication_date = utils.get_param_from_url(
        pdf_url, 'dt_publicacao')
      filepath = utils.get_filepath(publication_date, act_id, 'html')

      pdf_contents = self.pdf_contents(pdf_url, act_id, publication_date)
      yield [
        base.Content(
          content=doc.prettify(), dest=filepath, content_type='text/html'
        ),
        *pdf_contents
      ]

  def pdf_contents(self, pdf_path, act_id, publication_date):
    response = self.client.get(pdf_path)
    contents = []

    if 'text/html' in response.headers['Content-Type']:
      soup  = utils.soup_by_content(response.content)
      table = soup.find(id='listaInteiroTeor')
      rows  = table.find_all('div', class_='row')[1:]

      for index, row in enumerate(rows):
        url      = row.a['href'].replace('®', '&reg')
        if 'documento_sequencial' in url:
          doc_id   = utils.get_param_from_url(url, 'documento_sequencial')
        elif 'seq' in url:
          doc_id   = utils.get_param_from_url(url, 'seq')
        else:
          doc_id = 'NA'
        filename = f'{act_id}-{doc_id}--{index}'
        contents.append(base.ContentFromURL(
          src=url,
          dest=utils.get_filepath(
            date=publication_date, filename=filename, extension='pdf'
          ),
          content_type='application/pdf'
        ))

    else:
      pdf_filepath = utils.get_filepath(
        date=publication_date, filename=act_id, extension='pdf')

      contents.append(base.Content(
        content=response.content,
        content_type='application/pdf',
        dest=pdf_filepath
      ))

    return contents


@celery.task(queue='crawlers.stj', default_retry_delay=5 * 60,
             autoretry_for=(BaseException,))
def stj_task(start_date, end_date, output_uri):
  setup_cloud_logger(logger)

  from logutils import logging_context

  with logging_context(crawler='STJ'):
    output = utils.get_output_strategy_by_path(path=output_uri)
    logger.info(f'Output: {output}.')

    start_date, end_date =\
      pendulum.parse(start_date), pendulum.parse(end_date)

    query_params = {'start_date': start_date, 'end_date': end_date}
    collector = STJCollector(client=STJClient(), query=query_params)
    handler   = base.ContentHandler(output=output)

    snapshot = base.Snapshot(keys=query_params)
    base.get_default_runner(
        collector=collector, output=output, handler=handler, logger=logger, max_workers=8) \
      .run(snapshot=snapshot)


@cli.command(name='stj')
@click.option('--start-date', prompt=True,   help='Format YYYY-MM-DD.')
@click.option('--end-date'  , prompt=True,   help='Format YYYY-MM-DD.')
@click.option('--output-uri', default=None,  help='Output URI (e.g. gs://bucket_name')
@click.option('--enqueue'   , default=False, help='Enqueue for a worker'  , is_flag=True)
@click.option('--split-tasks',
  default=None, help='Split tasks based on time range (weeks, months, days, etc) (use with --enqueue)')
def stj_command(start_date, end_date, output_uri, enqueue, split_tasks):
  args = (start_date, end_date, output_uri)
  if enqueue:
    if split_tasks:
      start_date, end_date =\
        pendulum.parse(start_date), pendulum.parse(end_date)
      for start, end in reversed(list(utils.timely(start_date, end_date, unit=split_tasks, step=1))):
        task_id = stj_task.delay(
          start.to_date_string(),
          end.to_date_string(),
          output_uri)
        print(f"task {task_id} sent with params {start.to_date_string()} {end.to_date_string()}")
    else:
      stj_task.delay(*args)
  else:
    stj_task(*args)