from app.crawlers import base, utils
import math
import json
import pendulum
from app.crawlers.logconfig import logger_factory, setup_cloud_logger
import click
from app.crawler_cli import cli
from app.celery_run import celery_app as celery
import requests
import urllib
import base64

logger = logger_factory('tjrs')

SOURCE_DATE_FORMAT = 'DD/MM/YYYY'
DEFAULT_HEADERS = {
    'Accept': '*/*',
    'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6,ja;q=0.5',
    'Connection': 'keep-alive',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'Origin': 'https://www.tjrs.jus.br',
    'Referer': 'https://www.tjrs.jus.br/buscas/jurisprudencia/?conteudo_busca=&q_palavra_chave=&aba=jurisprudencia',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36 Edg/105.0.1343.27',
    'X-Requested-With': 'XMLHttpRequest',
    'sec-ch-ua': '"Microsoft Edge";v="105", " Not;A Brand";v="99", "Chromium";v="105"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
}


def merged_with_default_filters(start_date, end_date):
  return {
      'action': 'consultas_solr_ajax',
      'metodo': 'buscar_resultados',
      'parametros': {
          'aba': 'jurisprudencia',
          'realizando_pesquisa': 1,
          'pagina_atual': 1,
          'q_palavra_chave': '',
          'conteudo_busca': 'ementa_completa',
          'filtroComAExpressao': '',
          'filtroComQualquerPalavra': '',
          'filtroSemAsPalavras': '',
          'filtroTribunal': -1,
          'filtroRelator': -1,
          'filtroOrgaoJulgador': -1,
          'filtroTipoProcesso': -1,
          'filtroClasseCnj': -1,
          'assuntoCnj': -1,
          'filtroReferenciaLegislativa': '',
          'filtroJurisprudencia': '',
          'filtroComarcaOrigem': '',
          'filtroAssunto': '',
          'data_julgamento_de': f'{start_date}',
          'data_julgamento_ate': f'{end_date}',
          'filtroNumeroProcesso': '',
          'data_publicacao_de': '',
          'data_publicacao_ate': '',
          'filtroacordao': 'acordao',
          'wt': 'json',
          'ordem': 'asc,cod_documento%20asc,numero_processo%20asc,cod_ementa%20asc',
          'start': 0
      }
  }


class TJRSClient:

  def __init__(self):
    self.url = 'https://www.tjrs.jus.br/buscas/jurisprudencia/ajax.php?'

  @utils.retryable(max_retries=3)
  def count(self, filters):
    result = self.fetch(filters, page=1)
    return result['response']['numFound']

  def get_cookie(self, cookie_name):
    value = None
    cookies = self.driver.get_cookies()
    for cookie in cookies:
      if cookie.get('name') == cookie_name:
        value = cookie['value']
    if cookie is None:
      raise Exception(f'Cookie not found: {cookie_name}')
    return value

  @utils.retryable(max_retries=3)
  def fetch(self, filters, page=1):
    try:
      if not isinstance(filters['parametros'], str):
        filters['parametros']['pagina_atual'] = page
        filters['parametros'] = urllib.parse.urlencode(filters['parametros'], quote_via=urllib.parse.quote)
        return requests.post(self.url,
                             data=filters,
                             headers=DEFAULT_HEADERS,
                             verify=False
                             ).json()

    except Exception as e:
      logger.error(f"page fetch error params: {filters}")
      raise e


class TJRSCollector(base.ICollector):

  def __init__(self, client, filters):
    self.client = client
    self.filters = filters

  def count(self):
    return self.client.count(merged_with_default_filters(**self.filters))

  def chunks(self):
    total = self.count()
    pages = math.ceil(total/10)

    for page in range(1, pages + 1):
      yield TJRSChunk(
          keys={
              **self.filters, **{'page': page}
          },
          prefix='',
          filters=self.filters,
          page=page,
          client=self.client
      )


class TJRSChunk(base.Chunk):

  def __init__(self, keys, prefix, filters, page, client):
    super(TJRSChunk, self).__init__(keys, prefix)
    self.filters = filters
    self.page = page
    self.client = client

  def rows(self):
    result = self.client.fetch(merged_with_default_filters(**self.filters), self.page)
    if result is None:
      logger.error("ERROR - could not collect for {self.filters} - page {self.page}")
    else:
      for n, record in enumerate(result['response']['docs']):

        session_at = pendulum.parse(record['data_julgamento'])
        base_path = f'{session_at.year}/{session_at.month:02d}'

        if 'cod_documento' in record:
          codigo = record['cod_documento']
          ano = record['ano_criacao']
          numero = record['numero_processo']

          dest_record = f"{base_path}/doc_{numero}_{codigo}.json"

          report_url = f'https://www.tjrs.jus.br/site_php/consulta/download/exibe_documento_att.php?numero_processo={numero}&ano={ano}&codigo={codigo}'
          dest_report = f"{base_path}/doc_{numero}_{codigo}.doc"

          yield [
              base.Content(content=json.dumps(record), dest=dest_record,
                           content_type='application/json'),
              base.ContentFromURL(src=report_url, dest=dest_report,
                                  content_type='application/doc')
          ]

        else:
          numero = record['numero_processo']
          codigo = record['cod_ementa']

          dest_record = f"{base_path}/doc_{numero}_{codigo}.json"
          dest_report = f"{base_path}/doc_{numero}_{codigo}.html"

          extra_contents = []

          if 'documento_text_aspas' in record:
            record['documento_text_aspas'] = base64.b64decode(record['documento_text_aspas']).decode('latin-1')
          if 'documento_text' in record:
            logger.warn(f'File {dest_record} has no full document text')
            record['documento_text'] = base64.b64decode(record['documento_text']).decode('latin-1')
            extra_contents.append(base.Content(content=record['documento_text'], dest=dest_report,
                                               content_type='text/html')
                                  )
          else:
            logger.warn(f'File {dest_record} has no document text as well')

          yield [
              base.Content(content=json.dumps(record), dest=dest_record,
                           content_type='application/json'),
              *extra_contents
          ]


@celery.task(name='crawlers.tjrs', default_retry_delay=5 * 60,
             autoretry_for=(BaseException,))
def tjrs_task(**kwargs):
  setup_cloud_logger(logger)

  from app.crawlers.logutils import logging_context

  with logging_context(crawler='tjrs'):
    output = utils.get_output_strategy_by_path(path=kwargs.get('output_uri'))
    logger.info(f'Output: {output}.')

    start_date = pendulum.parse(kwargs.get('start_date')).format(SOURCE_DATE_FORMAT)
    end_date = pendulum.parse(kwargs.get('end_date')).format(SOURCE_DATE_FORMAT)

    filters = {
        'start_date': start_date,
        'end_date': end_date,
    }

    collector = TJRSCollector(client=TJRSClient(), filters=filters)
    handler = base.ContentHandler(output=output)
    snapshot = base.Snapshot(keys=filters)

    base.get_default_runner(
        collector=collector,
        output=output,
        handler=handler,
        logger=logger,
        max_workers=8) \
        .run(snapshot=snapshot)


@cli.command(name='tjrs')
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
def tjrs_command(**kwargs):
  if kwargs.get('enqueue'):
    del (kwargs['enqueue'])
    split_tasks = kwargs.get('split_tasks', None)
    del (kwargs['split_tasks'])
    utils.enqueue_tasks(tjrs_task, split_tasks, **kwargs)
  else:
    tjrs_task(*kwargs)
