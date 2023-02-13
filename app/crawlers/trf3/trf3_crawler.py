from app.crawlers import base, utils
import math
import pendulum
from app.crawlers.logconfig import logger_factory, setup_cloud_logger
import click
from app.crawler_cli import cli
from app.celery_run import celery_app as celery
import requests
from bs4 import BeautifulSoup
import re
import hashlib
from itertools import chain

DEFAULT_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:88.0) Gecko/20100101 Firefox/88.0',
    'Accept': '*/*',
    'Accept-Encoding': 'gzip, deflate',
    'Referer': 'https://web.trf3.jus.br/base-textual'
}

TRF3_DATE_FORMAT = 'DD/MM/YYYY'
DATE_PATTERN = re.compile(r'\d{2}/\d{2}/\d{4}')
FILES_PER_PAGE = 50

logger = logger_factory('trf3')


def nearest_date(items, pivot):
  pivot = pendulum.from_format(pivot, TRF3_DATE_FORMAT)
  if items and pivot:
    return min([pendulum.from_format(item.text, TRF3_DATE_FORMAT) for item in items],
               key=lambda x: abs(x - pivot))

  return ''


def get_content_hash(soup):
  content_string = ''.join(
      tag.text
      for tag in chain(
          soup.find_all('p',   {'class': 'docTexto'}),
          soup.find_all('div', {'class': 'docTexto'}),
          soup.find_all('pre', {'class': 'txtQuebra'}),
      )
  )
  return hashlib.sha1(content_string.encode('utf-8')).hexdigest()


def get_post_data(filters):
  return {
      'txtPesquisaLivre': '',
      'chkMostrarLista': 'on',
      'numero': '',
      'magistrado': 0,
      'data_inicial': pendulum.parse(filters.get('start_date')).format(TRF3_DATE_FORMAT),
      'data_final': pendulum.parse(filters.get('end_date')).format(TRF3_DATE_FORMAT),
      'data_tipo': 1,
      'classe': 0,
      'numclasse': '',
      'orgao': 0,
      'ementa': '',
      'indexacao': '',
      'legislacao': '',
      'chkAcordaos': 'on',
      'hdnMagistrado': '',
      'hdnClasse': '',
      'hdnOrgao': '',
      'hdnLegislacao': '',
      'hdnMostrarListaResumida': ''
  }


class TRF3Client:

  def __init__(self):
    self.session = requests.Session()

  @utils.retryable(max_retries=9, sleeptime=20)
  def setup(self):
    self.session.get('https://web.trf3.jus.br/base-textual',
                     headers=DEFAULT_HEADERS)

  @utils.retryable(max_retries=9, sleeptime=20)
  def count(self, filters):
    result = self.fetch(filters)
    soup = BeautifulSoup(result.text, features='html5lib')
    count = soup.find(
        'a', {'href': '/base-textual/Home/ListaResumida/1?np=0'})
    count = count.text if count else ''
    if count:
      return int(''.join([char for char in count if char.isdigit()]))
    else:
      return 0

  @utils.retryable(max_retries=9, sleeptime=20)
  def fetch(self, filters):
    self.setup()
    post_data = get_post_data(filters)
    url = 'https://web.trf3.jus.br/base-textual/Home/ResultadoTotais'
    return self.session.post(url,
                             data=post_data,
                             headers=DEFAULT_HEADERS)


class TRF3Collector(base.ICollector):

  def __init__(self, client, filters):
    self.client = client
    self.filters = filters

  @utils.retryable(max_retries=9, sleeptime=20)
  def count(self):
    return self.client.count(self.filters)

  @utils.retryable(max_retries=9, sleeptime=20)
  def chunks(self):
    total = self.count()
    pages = math.ceil(total/FILES_PER_PAGE)

    for page in range(1, pages + 2):
      yield TRF3Chunk(
          keys={**self.filters, **{'page': page}},
          prefix='',
          page=page,
          total=total,
          filters=self.filters,
          client=self.client,
      )


class TRF3Chunk(base.Chunk):

  def __init__(self, keys, prefix, page, total, filters, client):
    super(TRF3Chunk, self).__init__(keys, prefix)
    self.page = page
    self.total = total
    self.filters = filters
    self.client = client

  @utils.retryable(max_retries=9, sleeptime=10)
  def rows(self):
    self.client.fetch(self.filters)
    for proc_number in range(
        1 + ((self.page - 1) * FILES_PER_PAGE),
        1 + min(self.total, ((self.page) * FILES_PER_PAGE))
    ):
      to_download = []

      soup = self.get_page_soup(
          logger,
          url=f'https://web.trf3.jus.br/base-textual/Home/ListaColecao/9?np={proc_number}',
          headers=DEFAULT_HEADERS
      )

      # TODO: Refactor
      # Handle session expiration
      for n in range(1, 9):
        if soup.find('h2', text='Para iniciar uma nova sessão clique em algum dos links ao lado.'):
          logger.warn(f"Session expired - trying again. Retry #{n}...")
          self.client.setup()
          self.client.fetch(self.filters)
          soup = self.get_page_soup(
              logger,
              url=f'https://web.trf3.jus.br/base-textual/Home/ListaColecao/9?np={proc_number}',
              headers=DEFAULT_HEADERS
          )
        else:
          break
      else:
        raise Exception('Could not stablish session')

      pub_date_div = soup.find('div', text='Data da Publicação/Fonte ')
      pub_date, = DATE_PATTERN.findall(
          pub_date_div.next_sibling.next_sibling.text)

      data_julg_div = soup.find('div', text='Data do Julgamento ')
      session_at = data_julg_div.next_sibling.next_sibling.text.strip()
      session_at = pendulum.from_format(session_at, TRF3_DATE_FORMAT)

      processo_text = self.get_processo_text(soup)
      processo_num = ''.join(char for char in processo_text if char.isdigit())

      content_hash = utils.get_content_hash(soup,
                                            tag_descriptions=[
                                                {'name': 'p',    'class_': 'docTexto'},
                                                {'name': 'div',  'class_': 'docTexto'},
                                                {'name': 'pre',  'class_': 'txtQuebra'}
                                            ],
                                            length=40)

      dest_path = f'{session_at.year}/{session_at.month:02d}/{session_at.day:02d}_{processo_num}_{content_hash}.html'

      to_download.append(base.Content(
          content=BeautifulSoup(
              str(soup), features='html5lib').encode('latin-1'),
          dest=dest_path,
          content_type='text/html'))

      url_page_acordao = soup.find(
          'a', {'title': 'Exibir a íntegra do acórdão.'}).get('href')

      page_acordao_soup = self.get_page_soup(
          logger, url=url_page_acordao, headers=DEFAULT_HEADERS, processo_text=processo_text
      )

      link_date = nearest_date(page_acordao_soup.find_all(
          'a', text=re.compile('\d{2}/\d{2}/\d{4}')), pivot=pub_date)

      if link_date:
        link_to_inteiro = page_acordao_soup.find(
            'a', text=link_date.format(TRF3_DATE_FORMAT))
        dest_path_inteiro = f'{session_at.year}/{session_at.month:02d}/{session_at.day:02d}_{processo_num}_{content_hash}_INTEIRO.html'
        url_acordao_inteiro = link_to_inteiro.get('href')
        to_download.append(base.ContentFromURL(
            src=f'https://web.trf3.jus.br{url_acordao_inteiro}',
            dest=dest_path_inteiro,
            content_type='text/html'
        ))
      else:

        logger.error(
            f'Link not available for full document of: {processo_text}')

      yield to_download

  def get_processo_text(self, soup):
    return soup.find('h4', text='Processo').next_sibling.next_sibling.text.strip()

  @utils.retryable(max_retries=9, sleeptime=10, ignore_if_exceeds=True)
  def get_page_soup(self, logger, url, headers, processo_text=''):
    page_response = utils.get_response(
        logger=logger,
        url=url,
        headers=headers,
        session=self.client.session)
    page_acordao_soup = BeautifulSoup(page_response.text, features='html5lib')
    self.page_is_error(logger, page_acordao_soup, processo_text)
    return page_acordao_soup

  def page_is_error(self, logger, soup, processo_text):
    error_div = soup.find(name='div', id='erro')
    if error_div is not None:
      is_error = error_div.find(text=re.compile(r'^[\s\n]*Ocorreu[\s\n]*um[\s\n]*erro\.?[\s\n]*$'))
      if is_error:
        logger.error(f'Link not available for full document of: {processo_text} - retrying...')
        raise utils.PleaseRetryException()


class TRF3Handler(base.ContentHandler):
  def __init__(self, output, headers):
    super(TRF3Handler, self).__init__(output)
    self.headers = headers

  @utils.retryable(max_retries=9, sleeptime=10)
  def _handle_url_event(self, event):

    if self.output.exists(event.dest):
      return

    try:
      response = requests.get(event.src,
                              allow_redirects=True,
                              headers=self.headers,
                              verify=False)

      dest = event.dest
      content_type = event.content_type

      if response.status_code == 200:
        self.output.save_from_contents(
            filepath=dest,
            contents=response.content,
            content_type=content_type)
      else:
        logger.warn(f"Response <{response.status_code}> - {response.url}")
        raise utils.PleaseRetryException()
    except requests.exceptions.Timeout as e:
      logger.error(f"Timeout Error: {e} - {event.src}")
    except requests.exceptions.ConnectionError as e:
      logger.error(f"Connection Error: {e} - {event.src}")
    except requests.exceptions.RequestException as e:
      logger.error(f"General Request Error: {e} - {event.src}")


@celery.task(name='crawlers.trf3', default_retry_delay=5 * 60,
             autoretry_for=(BaseException,))
def trf3_task(**kwargs):
  setup_cloud_logger(logger)

  from app.crawlers.logutils import logging_context

  with logging_context(crawler='trf3'):
    output = utils.get_output_strategy_by_path(
        path=kwargs.get('output_uri'))
    logger.info(f'Output: {output}.')

    query_params = {
        'start_date': kwargs.get('start_date'),
        'end_date': kwargs.get('end_date')
    }

    collector = TRF3Collector(client=TRF3Client(), filters=query_params)
    handler = TRF3Handler(output=output, headers=DEFAULT_HEADERS)
    snapshot = base.Snapshot(keys=query_params)

    base.get_default_runner(
        collector=collector,
        output=output,
        handler=handler,
        logger=logger,
        max_workers=8) \
        .run(snapshot=snapshot)


@cli.command(name='trf3')
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
def trf3_command(**kwargs):
  enqueue, split_tasks = kwargs.get('enqueue'), kwargs.get('split_tasks')
  del (kwargs['enqueue'])
  del (kwargs['split_tasks'])
  if enqueue:
    utils.enqueue_tasks(trf3_task, split_tasks, **kwargs)
  else:
    trf3_task(**kwargs)
