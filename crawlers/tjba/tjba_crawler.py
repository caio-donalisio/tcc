import base
import math
import json
import utils
import pendulum
import click
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport

from app import cli, celery

from logconfig import logger_factory, setup_cloud_logger
logger = logger_factory('tjba')


with open(f'crawlers/tjba/query.graphql', 'r') as f:
  graphql_query = gql(f.read())


def get_filters(start_date : pendulum.DateTime, end_date : pendulum.DateTime):
  return {
    'assunto': 'a OR o OR de OR por',
    'orgaos': [],
    'relatores': [],
    'classes': [],
    'dataInicial': start_date.start_of('day').to_iso8601_string(),
    'dataFinal': end_date.end_of('day').to_iso8601_string(),
    'segundoGrau': True,
    'turmasRecursais': True,
    'ordenadoPor': 'dataPublicacao'
  }


class TJBAClient:
  def __init__(self):
    self.transport  = RequestsHTTPTransport(
      url="https://jurisprudenciaws.tjba.jus.br/graphql",
      verify=False, retries=3, timeout=60)
    self.gql_client = Client(transport=self.transport)

  @utils.retryable(max_retries=3)
  def count(self, filters):
    result = self.fetch(filters, page_number=0, items_per_page=1)
    return result['filter']['itemCount']

  @utils.retryable(max_retries=3)
  def fetch(self, filters, page_number=0, items_per_page=10):
    try:
      params = {
        'decisaoFilter': filters,
        'pageNumber': page_number,
        'itemsPerPage': items_per_page,
      }
      return self.gql_client.execute(graphql_query, variable_values=params)
    except Exception as e:
      logger.error(f"page fetch error params: {params}")
      raise e

  @utils.retryable(max_retries=3)
  def paginator(self, filters, items_per_page=10):
    item_count = self.count(filters)
    page_count = math.ceil(item_count / items_per_page)
    return Paginator(self, filters=filters, item_count=item_count, page_count=page_count,
      items_per_page=items_per_page)


class Paginator:
  def __init__(self, client, filters, item_count, page_count, items_per_page=10):
    self.client = client
    self._filters = filters
    self._item_count = item_count
    self._page_count = page_count
    self._items_per_page = items_per_page

  @property
  def total(self):
    return self._item_count

  @property
  def pages(self):
    return self._page_count

  def page(self, number):
    return self.client.fetch(
      filters=self._filters, page_number=number, items_per_page=self._items_per_page)

  def __repr__(self):
    return f'Paginator(item_count={self._item_count}, page_count={self._page_count})'


class TJBACollector(base.ICollector):

  def __init__(self, client : TJBAClient, query : dict, **options):
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
      keys =\
        {'start_date': start_date.to_date_string(),
         'end_date'  : end_date.to_date_string()}

      yield TJBAChunk(keys=keys,
        client=self.client, filters=get_filters(start_date, end_date))


class TJBAChunk(base.Chunk):

  def __init__(self, keys, client, filters):
    super(TJBAChunk, self).__init__(keys)
    self.client  = client
    self.filters = filters

  @utils.retryable(max_retries=3)
  def rows(self):
    count  = self.client.count(self.filters)
    if count == 0:
      return

    result = self.client.fetch(self.filters, items_per_page=count)

    for record in result['filter']['decisoes']:
      published_at = pendulum.parse(record['dataPublicacao'])
      doc_hash = record['hash']
      doc_id   = record['id']

      base_path   = f'{published_at.year}/{published_at.month:02d}'
      dest_record = f"{base_path}/doc_{doc_id}_{doc_hash}.json"
      dest_report = f"{base_path}/doc_{doc_id}_{doc_hash}_report.html"
      report_url  =\
        f'https://jurisprudenciaws.tjba.jus.br/inteiroTeor/{doc_hash}'

      yield [
        base.Content(content=json.dumps(record), dest=dest_record,
          content_type='application/json'),
        base.ContentFromURL(src=report_url, dest=dest_report, content_type='text/html')
      ]


@celery.task(queue='crawlers.tjba', default_retry_delay=5 * 60,
             autoretry_for=(BaseException,))
def tjba_task(start_date, end_date, output_uri):
  setup_cloud_logger(logger)

  from logutils import logging_context

  with logging_context(crawler='tjba'):
    output = utils.get_output_strategy_by_path(path=output_uri)
    logger.info(f'Output: {output}.')

    start_date, end_date =\
      pendulum.parse(start_date), pendulum.parse(end_date)

    collector = TJBACollector(client=TJBAClient(),
      query={'start_date': start_date, 'end_date': end_date})

    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
      handler = base.ContentHandler(output=output)
      manager = base.ChunkStateManager(output=output)
      processor =\
        base.FutureChunkProcessor(executor=executor,
          handler=handler, manager=manager)

      runner = base.ChunkRunner(
        collector=collector,
        processor=processor,
        logger=logger
      )
      runner.run()


@cli.command(name='tjba')
@click.option('--start-date', prompt=True,   help='Format YYYY-MM-DD.')
@click.option('--end-date'  , prompt=True,   help='Format YYYY-MM-DD.')
@click.option('--output-uri', default=None,  help='Output URI (e.g. gs://bucket_name')
@click.option('--enqueue'   , default=False, help='Enqueue for a worker'  , is_flag=True)
@click.option('--split-tasks',
  default=None, help='Split tasks based on time range (weeks, months, days, etc) (use with --enqueue)')
def tjba_command(start_date, end_date, output_uri, enqueue, split_tasks):
  args = (start_date, end_date, output_uri)
  if enqueue:
    if split_tasks:
      start_date, end_date =\
        pendulum.parse(start_date), pendulum.parse(end_date)
      for start, end in utils.timely(start_date, end_date, unit=split_tasks, step=1):
        task_id = tjba_task.delay(
          start.to_date_string(),
          end.to_date_string(),
          output_uri)
        print(f"task {task_id} sent with params {start.to_date_string()} {end.to_date_string()}")
    else:
      tjba_task.delay(*args)
  else:
    tjba_task(*args)