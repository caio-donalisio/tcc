from app.crawlers import base, utils
import pendulum
from app.crawlers.logconfig import logger_factory, setup_cloud_logger
import click
from app.crawler_cli import cli
from app.celery_run import celery_app as celery
import requests
import json

logger = logger_factory('tjmt')

COURT_NAME = 'tjmt'
RESULTS_PER_PAGE = 50
INPUT_DATE_FORMAT = 'YYYY-MM-DD'
SEARCH_DATE_FORMAT = 'YYYY-MM-DD'
NOW = pendulum.now()

DEFAULT_HEADERS = {
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6,ja;q=0.5',
    'Connection': 'keep-alive',
    'Origin': 'https://jurisprudencia.tjmt.jus.br',
    'Referer': 'https://jurisprudencia.tjmt.jus.br/',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-site',
    'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Mobile Safari/537.36 Edg/111.0.1661.44',
    'sec-ch-ua': '"Microsoft Edge";v="111", "Not(A:Brand";v="8", "Chromium";v="111"',
    'sec-ch-ua-mobile': '?1',
    'sec-ch-ua-platform': '"Android"',
}

def get_filters(start_date, end_date, page=1, **kwargs):
  return {
    'filtro.isBasica': 'true',
    'filtro.indicePagina': str(page),
    'filtro.quantidadePagina': str(RESULTS_PER_PAGE),
    'filtro.tipoConsulta': 'Acordao',
    'filtro.termoDeBusca': ' ',
    'filtro.area': '',
    'filtro.numeroProtocolo': '',
    'filtro.periodoDataDe': start_date.format(SEARCH_DATE_FORMAT),
    'filtro.periodoDataAte': end_date.format(SEARCH_DATE_FORMAT),
    'filtro.tipoBusca': '1',
    'filtro.relator': '',
    'filtro.julgamento': '',
    'filtro.orgaoJulgador': '',
    'filtro.colegiado': 'Segunda',
    'filtro.localConsultaAcordao': '',
    'filtro.fqOrgaoJulgador': '',
    'filtro.fqTipoProcesso': '',
    'filtro.fqRelator': '',
    'filtro.fqJulgamento': '',
    'filtro.fqAssunto': '',
    'filtro.ordenacao.ordenarPor': 'DataDecrescente',
    'filtro.ordenacao.ordenarDataPor': 'Publicacao',
    'filtro.tipoProcesso': '',
    'filtro.thesaurus': 'false',
    'filtro.fqTermos': 'undefined',
}

class NoProcessNumberError(Exception):
  pass

class TJMTClient:

  def __init__(self):
    self.session = requests.Session()
    self.url = f'https://jurisprudencia-api.tjmt.jus.br/api/Consulta'
    self.session.get('https://jurisprudencia.tjmt.jus.br/catalogo', verify=False)

  @utils.retryable(max_retries=3)
  def count(self, filters):
    result = self.fetch(filters)
    count = int(result.json()['CountAcordaoDocumento'])
    return count

  @utils.retryable(max_retries=3)
  def fetch(self, filters, page=1):
    self.session.headers.update(DEFAULT_HEADERS)
    response = requests.get(
      url=self.url, 
      params=get_filters(filters['start_date'], filters['end_date'], page),
      verify=False,
      )
    response.raise_for_status()
    return response

class TJMTCollector(base.ICollector):

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
      yield TJMTChunk(
          keys={
              'start_date': self.filters['start_date'].to_date_string(),
              'end_date': self.filters['end_date'].to_date_string(),
              'page': page,
              'count': total,
              'results_per_page':str(RESULTS_PER_PAGE),
          },
          prefix='',
          filters=self.filters,
          page=page,
          client=self.client,
      )

class TJMTChunk(base.Chunk):
  def __init__(self, keys, client, filters, prefix, page):
    super(TJMTChunk, self).__init__(keys, prefix)
    self.client = client
    self.filters = filters
    self.page = page

  @utils.retryable(max_retries=3)
  def rows(self):
    rows = self.client.fetch(self.filters, self.page)
    for row in rows.json()['AcordaoCollection']:
      to_download = []
      filepath = self.get_filepath(row)
      inteiro = row.get('Documento')
      if inteiro:
        del row['Documento']
        to_download.append(
          base.Content(
          content=str(inteiro),
          dest=f'{filepath}.html',
          content_type='text/html'))
      to_download.append(
        base.Content(
        content=json.dumps(row), 
        dest=f'{filepath}.json',
        content_type='application/json'))
      yield to_download

  def get_filepath(self, row):
    default_value = '0' * 10
    proc_id = row['Id']
    proc_number = row['Processo'].get('NumeroUnico', default_value)
    date = row['Processo'].get('DataPublicacaoFormatada')
    if not date:
      logger.warn(f'Data de publicação not found for {proc_id}. Trying to get Data de Julgamento')
      date = row['Processo']['DataJulgamentoFormatada']
    day, month, year = date.split('/')
    assert pendulum.from_format(date,'DD/MM/YYYY')
    import random, string
    return f'{year}/{month}/{day}_{proc_id}_{proc_number}_TJMT'


@celery.task(name='crawlers.tjmt', default_retry_delay=5 * 60,
             autoretry_for=(BaseException,))
def tjmt_task(**kwargs):
  setup_cloud_logger(logger)

  from app.crawlers.logutils import logging_context

  with logging_context(crawler='tjmt'):
    output = utils.get_output_strategy_by_path(path=kwargs.get('output_uri'))
    logger.info(f'Output: {output}.')

    query_params = {
        'start_date': pendulum.from_format(kwargs.get('start_date'), INPUT_DATE_FORMAT),
        'end_date': pendulum.from_format(kwargs.get('end_date'), INPUT_DATE_FORMAT),
    }

    collector = TJMTCollector(client=TJMTClient(), filters=query_params)
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
def tjmt_command(**kwargs):
  enqueue, split_tasks = kwargs.get('enqueue'), kwargs.get('split_tasks')
  del (kwargs['enqueue'])
  del (kwargs['split_tasks'])
  if enqueue:
    utils.enqueue_tasks(tjmt_task, split_tasks, **kwargs)
  else:
    tjmt_task(**kwargs)
