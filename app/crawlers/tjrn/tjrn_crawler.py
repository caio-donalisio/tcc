from app.crawlers import base, utils
import pendulum
from app.crawlers.logconfig import logger_factory, setup_cloud_logger
import click
from app.crawler_cli import cli
from app.celery_run import celery_app as celery
import requests
import json

logger = logger_factory('tjrn')

COURT_NAME = 'tjrn'
RESULTS_PER_PAGE = 10  # DEFINED BY THEIR SERVER, SHOULDN'T BE CHANGED
INPUT_DATE_FORMAT = 'YYYY-MM-DD'
SEARCH_DATE_FORMAT = 'DD-MM-YYYY'
NOW = pendulum.now()
BASE_URL = 'https://jurisprudencia.tjrn.jus.br/'

DEFAULT_HEADERS = {
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6,ja;q=0.5',
    'Connection': 'keep-alive',
    'Content-Type': 'application/json;charset=UTF-8',
    # 'Cookie': '_ga=GA1.3.212902379.1678477803; _gid=GA1.3.2061417349.1678477803; _gat_gtag_UA_118963421_1=1',
    'Origin': 'https://jurisprudencia.tjrn.jus.br',
    'Referer': 'https://jurisprudencia.tjrn.jus.br/',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Mobile Safari/537.36 Edg/110.0.1587.63',
    'X-Requested-With': 'XMLHttpRequest',
    'sec-ch-ua': '"Chromium";v="110", "Not A(Brand";v="24", "Microsoft Edge";v="110"',
    'sec-ch-ua-mobile': '?1',
    'sec-ch-ua-platform': '"Android"',
}

def get_filters(start_date, end_date, page=1, **kwargs):
  return{
    "jurisprudencia": {
        "ementa": "",
        "inteiro_teor": "",
        "nr_processo": "",
        "id_classe_judicial": "",
        "id_orgao_julgador": "",
        "id_relator": "",
        "id_colegiado": "",
        "id_juiz": "",
        "id_vara": "",
        "dt_inicio": start_date.format(SEARCH_DATE_FORMAT),
        "dt_fim": end_date.format(SEARCH_DATE_FORMAT),
        "origem": "",
        "sistema": "",
        "decisoes": "Acórdão",
        "jurisdicoes": "TJ",
        "grau": ""
    },
    "page": page,
    "usuario": {
        "matricula": "",
        "token": ""
    }
}

class NoProcessNumberError(Exception):
  pass


class TJRNClient:

  def __init__(self):
    self.session = requests.Session()
    self.url = f'https://jurisprudencia.tjrn.jus.br/api/pesquisar'
    self.session.get('https://jurisprudencia.tjrn.jus.br', verify=False)

  @utils.retryable(max_retries=3)
  def count(self, filters):
    result = self.fetch(filters)
    return result.json()['hits']['total']

  @utils.retryable(max_retries=3)
  def fetch(self, filters, page=1):
    self.session.headers.update(DEFAULT_HEADERS)
    response = requests.post(
      url=self.url, 
      json=get_filters(filters['start_date'], filters['end_date'], page),
      verify=False,
      )
    response.raise_for_status()
    return response

class TJRNCollector(base.ICollector):

  def __init__(self, client, filters):
    self.client = client
    self.filters = filters

  def count(self, period=None):
    return self.client.count(self.filters)

  @utils.retryable()
  def chunks(self):
    total = self.count(self.filters)
    pages = range(1, 2 + total//RESULTS_PER_PAGE)
    for page in pages:
      yield TJRNChunk(
          keys={
              'start_date': self.filters['start_date'].to_date_string(),
              'end_date': self.filters['end_date'].to_date_string(),
              'page': page,
              'count': total,
          },
          prefix='',
          filters=self.filters,
          page=page,
          client=self.client,
      )

class TJRNChunk(base.Chunk):
  def __init__(self, keys, client, filters, prefix, page):
    super(TJRNChunk, self).__init__(keys, prefix)
    self.client = client
    self.filters = filters
    self.page = page

  @utils.retryable(max_retries=3)
  def rows(self):
    rows = self.client.fetch(self.filters, self.page)
    for row in rows.json()['hits']['hits']:
      yield [base.Content(
        content=json.dumps(row), 
        dest=self.get_filepath(row), 
        content_type='application/json')
      ]

  def get_filepath(self, row):
    default_value = '0' * 10
    date = row['_source']['dt_assinatura_teor']
    year, month, day = date.split('-')
    assert pendulum.from_format(date,'YYYY-MM-DD')
    process = row['_source'].get('numero_processo', default_value)
    id_ementa = row['_source'].get('id_documento_ementa', default_value)
    id_teor = row['_source'].get('id_documento_teor', default_value)
    return f'{year}/{month}/{day}_{process}_{id_ementa}_{id_teor}.json'


@celery.task(name='crawlers.tjrn', default_retry_delay=5 * 60,
             autoretry_for=(BaseException,))
def tjrn_task(**kwargs):
  setup_cloud_logger(logger)

  from app.crawlers.logutils import logging_context

  with logging_context(crawler='tjrn'):
    output = utils.get_output_strategy_by_path(path=kwargs.get('output_uri'))
    logger.info(f'Output: {output}.')

    start_date = kwargs.get('start_date')
    start_date = pendulum.from_format(start_date, INPUT_DATE_FORMAT)  # .format(SEARCH_DATE_FORMAT)
    end_date = kwargs.get('end_date')
    end_date = pendulum.from_format(end_date, INPUT_DATE_FORMAT)  # .format(SEARCH_DATE_FORMAT)

    query_params = {
        'start_date': start_date,
        'end_date': end_date,
    }

    collector = TJRNCollector(client=TJRNClient(), filters=query_params)
    handler = base.ContentHandler(output=output)
    snapshot = base.Snapshot(keys=query_params)

    base.get_default_runner(
        collector=collector,
        output=output,
        handler=handler,
        logger=logger,
        skip_cache=kwargs.get('skip_cache'),
        max_workers=8) \
        .run(snapshot=snapshot)


@cli.command(name=COURT_NAME)
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
@click.option('--skip-cache' ,    default=False,    help='Starts collection from the beginning'  , is_flag=True)
def tjrn_command(**kwargs):
  enqueue, split_tasks = kwargs.get('enqueue'), kwargs.get('split_tasks')
  del (kwargs['enqueue'])
  del (kwargs['split_tasks'])
  if enqueue:
    utils.enqueue_tasks(tjrn_task, split_tasks, **kwargs)
  else:
    tjrn_task(**kwargs)
