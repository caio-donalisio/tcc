import base
import json
import utils
import pendulum
import click
from app import cli, celery

from crawlers.tjba.tjba_crawler import TJBAClient, get_filters

from logconfig import logger_factory, setup_cloud_logger
logger = logger_factory('tjba')


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
        processor=processor
      )
      runner.run()



@cli.command(name='tjba2')
@click.option('--start-date', prompt=True,   help='Format YYYY-MM-DD.')
@click.option('--end-date'  , prompt=True,   help='Format YYYY-MM-DD.')
@click.option('--output-uri', default=None,  help='Output URI (e.g. gs://bucket_name')
@click.option('--enqueue'   , default=False, help='Enqueue for a worker'  , is_flag=True)
@click.option('--split-tasks',
  default=None, help='Split tasks based on time range (weeks, months, days, etc) (use with --enqueue)')
def tjba_command(start_date, end_date, output_uri, enqueue, split_tasks):
  args = (start_date, end_date, output_uri)
  tjba_task(*args)