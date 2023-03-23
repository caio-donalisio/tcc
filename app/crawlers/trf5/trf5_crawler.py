from app.crawlers import base, utils
import math
import json
import pendulum
from app.crawlers.logconfig import logger_factory
import click
from app.crawler_cli import cli
from app.celery_run import celery_app as celery
import requests
import re

logger = logger_factory('trf5')

SOURCE_DATE_FORMAT = 'DD/MM/YYYY'
DEFAULT_HEADERS = {
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6,ja;q=0.5',
    'Connection': 'keep-alive',
    # Requests sorts cookies= alphabetically
    # 'Cookie': '_ga=GA1.1.1066051638.1658924752; _ga_R55DNGGL90=GS1.1.1661798142.4.0.1661799185.0.0.0; 5a292a765166d38b90f5e9ce17f2d2c3=54598a02fcdfe5708f06c0b18a3ea24c; trf501e84bc9=01aa396bb16debe85417de9d25c154491ec88b4f7f3d70cbd506d2e8efb81380e7a4a1c5d3de987448cb8df426b172d0e4b5d7d1a32bf3286b6a1527b86e2774613a95904e; trf5361cd1e2027=08e38928b9ab2000decbaabcb5dd40ceb2083b78cd2ed287f13c37916de8a45302bd7db4093c90ac083e1db553113000b5912f217d7b598b5c0c509dceefbc7931e80196e8aa15cbc85ba20dbb520a5a1970cf377819db894b65ee9e137417e4',
    'Referer': 'https://juliapesquisa.trf5.jus.br/julia-pesquisa/',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36 Edg/105.0.1343.27',
    'X-Requested-With': 'XMLHttpRequest',
    'sec-ch-ua': '"Microsoft Edge";v="105", " Not;A Brand";v="99", "Chromium";v="105"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
}

def merged_with_default_filters(start_date, end_date, **kwargs):
    return {
        'draw': '1',
        'columns[0][data]': 'codigoDocumento',
        'columns[0][name]': '',
        'columns[0][searchable]': 'true',
        'columns[0][orderable]': 'false',
        'columns[0][search][value]': '',
        'columns[0][search][regex]': 'false',
        'start': '0',
        'length': '10',
        'search[value]': '',
        'search[regex]': 'false',
        'pesquisaLivre': '',
        'numeroProcesso': '',
        'orgaoJulgador': '',
        'relator': '',
        'dataIni': f'{start_date}',
        'dataFim': f'{end_date}',
    }


class TRF5Client:

  def __init__(self):
    self.url = 'https://juliapesquisa.trf5.jus.br/julia-pesquisa/api/documentos:dt'

  @utils.retryable(max_retries=6)
  def count(self, filters):
    result = self.fetch(filters, page=1)
    return result['recordsTotal']

  @utils.retryable(max_retries=6)
  def fetch(self, filters, page=1, per_page=10):
    try:
      filters['start'] = (page * per_page) - per_page
      filters['length'] = per_page

      response = requests.get(
          self.url,
          params=filters,
          headers=DEFAULT_HEADERS
      )
      return response.json()
# https://juliapesquisa.trf5.jus.br/julia-pesquisa/api/documentos:dt
    except Exception as e:
      logger.error(f"page fetch error params: {filters}")
      raise e


class TRF5Collector(base.ICollector):

  def __init__(self, client, filters):
    self.client = client
    self.filters = filters

  def count(self):
    return self.client.count(merged_with_default_filters(**self.filters))

  def chunks(self):
    total = self.count()
    pages = math.ceil(total/10)

    for page in range(1, pages + 1):
      yield TRF5Chunk(
          keys={
              **self.filters, **{'page': page, 'count': total}
          },
          prefix='',
          filters=self.filters,
          page=page,
          client=self.client
      )


class TRF5Chunk(base.Chunk):

  def __init__(self, keys, prefix, filters, page, client):
    super(TRF5Chunk, self).__init__(keys, prefix)
    self.filters = filters
    self.page = page
    self.client = client

  def rows(self):

    from app.crawlers.trf5 import trf5_pdf

    result = self.client.fetch(merged_with_default_filters(**self.filters), self.page)
    for _, record in enumerate(result['data']):

      session_at = pendulum.parse(record['dataJulgamento'])
      codigo = re.sub(r"\:", "-", record['codigoDocumento'])
      numero = record['numeroProcesso']

      base_path = f'{session_at.year}/{session_at.month:02d}'
      doc_base_path = f"{base_path}/doc_{numero}_{codigo}"

      dest_record = f"{doc_base_path}.json"
      to_download = []

      to_download.append(base.Content(content=json.dumps(record), dest=dest_record,
                                      content_type='application/json'))

      if not self.filters.get('skip_full'):
        report = trf5_pdf.TRF5Downloader()._get_report_url(record)

        if report.get('url') is None:
          logger.warn(f"Not found 'Inteiro Teor' for judgment {record['numeroProcesso']}")

        dest_report = None
        if 'html' in report.get('content_type', []):
          dest_report = f"{doc_base_path}.html"
        elif 'pdf' in report.get('content_type', []):
          dest_report = f"{doc_base_path}.pdf"

        if report.get('url') and dest_report:
          to_download.append(base.ContentFromURL(
              src=report['url'],
              dest=dest_report,
              content_type=report['content_type']
          ))
        else:
          logger.warn("Missing required values to append download task")

      yield to_download


@celery.task(name='crawlers.trf5', default_retry_delay=5 * 60,
             autoretry_for=(BaseException,))
def trf5_task(**kwargs):
  from app.crawlers.logutils import logging_context

  with logging_context(crawler='trf5'):
    output = utils.get_output_strategy_by_path(path=kwargs.get('output_uri'))
    logger.info(f'Output: {output}.')

    start_date = pendulum.parse(kwargs.get('start_date')).format(SOURCE_DATE_FORMAT)
    end_date = pendulum.parse(kwargs.get('end_date')).format(SOURCE_DATE_FORMAT)

    filters = {
            'start_date' :start_date,
            'end_date': end_date,
            'skip_full': kwargs.get('skip_full'),
        }
    collector = TRF5Collector(
        client=TRF5Client(),
        filters=filters
    )
    handler   = base.ContentHandler(output=output)
    snapshot = base.Snapshot(keys=filters)
    

    base.get_default_runner(
        collector=collector,
        output=output,
        handler=handler,
        logger=logger,
        max_workers=8,
        skip_cache=kwargs.get('skip_cache', False)) \
        .run(snapshot=snapshot)


@cli.command(name='trf5')
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
@click.option('--skip-full' ,    default=False,    help='Collects metadata only'  , is_flag=True)
@click.option('--skip-cache' ,    default=False,    help='Starts collection from the beginning'  , is_flag=True)
def trf5_command(**kwargs):
  enqueue, split_tasks = kwargs.get('enqueue'), kwargs.get('split_tasks')
  del (kwargs['enqueue'])
  del (kwargs['split_tasks'])
  if enqueue:
    utils.enqueue_tasks(trf5_task, split_tasks, **kwargs)
  else:
    trf5_task(**kwargs)
