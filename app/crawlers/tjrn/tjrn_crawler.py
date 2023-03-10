from app.crawlers import base, utils
import re
import pendulum
from app.crawlers.logconfig import logger_factory, setup_cloud_logger
import click
from app.crawler_cli import cli
from app.celery_run import celery_app as celery
import requests

logger = logger_factory('tjrn')

COURT_NAME = 'tjrn'
RESULTS_PER_PAGE = 10  # DEFINED BY THEIR SERVER, SHOULDN'T BE CHANGED
INPUT_DATE_FORMAT = 'YYYY-MM-DD'
SEARCH_DATE_FORMAT = 'DD/MM/YYYY'
NOW = pendulum.now()
BASE_URL = 'http://esaj.tjrn.jus.br/cjosg/'

DEFAULT_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6,ja;q=0.5',
    'Connection': 'keep-alive',
    # 'Cookie': 'JSESSIONID=260C058174AB7B85368335FDC4D266C0.oldapp1',
    'Referer': 'http://esaj.tjrn.jus.br/cjosg/pcjoPesquisa.jsp?tpClasse=J&deEmenta=+&clDocumento=&nuProcesso=&deClasse=&cdClasse=&deOrgaoJulgador=&cdOrgaoJulgador=&nmRelator=&cdRelator=&dtInicio=01%2F01%2F2018&dtTermino=31%2F01%2F2018&cdOrigemDoc=1&Submit=Pesquisar&rbCriterioEmenta=TODAS&rbCriterioBuscaLivre=TODAS&primeiroCodigo=10',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Mobile Safari/537.36 Edg/110.0.1587.63',
}

def get_filters(start_date, end_date, **kwargs):
  return {
    'tpClasse': 'J',
    'deEmenta': ' ',
    'clDocumento': '',
    'nuProcesso': '',
    'deClasse': '',
    'cdClasse': '',
    'deOrgaoJulgador': '',
    'cdOrgaoJulgador': '',
    'nmRelator': '',
    'cdRelator': '',
    'dtInicio': start_date.format(SEARCH_DATE_FORMAT),
    'dtTermino': end_date.format(SEARCH_DATE_FORMAT),
    'cdOrigemDoc': '1',
    'Submit': 'Pesquisar',
    'rbCriterioEmenta': 'TODAS',
    'rbCriterioBuscaLivre': 'TODAS',
    # 'primeiroCodigo': '0',
}

class NoProcessNumberError(Exception):
  pass


class TJRNClient:

  def __init__(self):
    self.session = requests.Session()
    # self.session.get('')
    self.url = f'http://esaj.tjrn.jus.br/cjosg/cjosg/pcjoPesquisa.jsp'

  @utils.retryable(max_retries=3)
  def count(self, filters, min_votes=3):
    counts = []
    while not any(counts.count(n) >= min_votes for n in counts):
      result = self.fetch(filters)
      soup = utils.soup_by_content(result.text)
      count_tag = soup.find('div', attrs={'class': 'navLeft'})
      if count_tag:
        count = re.search(r'^\s*(\d+) registro.*$', count_tag.text)
        count = int(count.group(1))
      else:
        count = 0
      counts.append(count)
    return max(counts, key=lambda n: counts.count(n))

  @utils.retryable(max_retries=3)
  def fetch(self, filters, page=1):
    self.session.headers.update(DEFAULT_HEADERS)
    try:
      return requests.get(
          url=f'{self.url}',
          params={
              **get_filters(**filters),
              'primeiroCodigo': (page-1)*10
          },
          headers=DEFAULT_HEADERS,
          verify=False,
          
      )

    except Exception as e:
      logger.error(f"page fetch error params: {filters}")
      raise e


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
              'end_date': self.filters['start_date'].to_date_string(),
              'page': page,
              'count': total,
          },
          prefix='',
          filters=self.filters,
          page=page,
          client=self.client
      )


class TJRNChunk(base.Chunk):

  def __init__(self, keys, prefix, filters, page, client):
    super(TJRNChunk, self).__init__(keys, prefix)
    self.filters = filters
    self.page = page
    self.client = client

  @utils.retryable()
  def rows(self):
    ...
    result = self.client.fetch(self.filters, self.page)
    print(5)

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
