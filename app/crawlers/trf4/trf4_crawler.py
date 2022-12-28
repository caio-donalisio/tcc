import re
import math
import time
import random
import requests
import pendulum
import sentry_sdk
from collections import defaultdict
from celery_singleton import Singleton

from app.crawlers import base, utils
import click
from app.crawler_cli import cli
from app.celery_run import celery_app as celery
from app.crawlers.logconfig import logger_factory, setup_cloud_logger

logger = logger_factory('trf4')


BASE_URL = 'https://jurisprudencia.trf4.jus.br/pesquisa'
DATE_FORMAT = '%d/%m/%Y'


class TRF4(base.BaseCrawler):
  def __init__(self, params, output, logger, **options):
    super(TRF4, self).__init__(params, output, logger, **options)
    self.requester = requests.Session()
    self.header_generator = utils.HeaderGenerator(
      origin='https://jurisprudencia.trf4.jus.br', xhr=False)
    self.referendaries = None
    self.orgs          = None
    self.classes       = None

  def run_for_seq(self, start, end):
    ids = []
    for seq in range(int(start), int(end)):
      ids.append(f'TRF4{seq}')
    logger.info(f'Fetching records from {len(ids)}.')

    params = {}

    partition_size = 50
    partitions  = [ids[i:i + partition_size] for i in range(0, len(ids), partition_size)]
    logger.info(f'Partitions {len(partitions)}.')

    def rows_generator(pagination, params, records):
      yield from self._rows_from_pagination(pagination, params, records)

    chunks = []
    for partition in partitions:
      records = len(partition)
      pagination = {
        'total': records,
        'vetPaginacao': ','.join(partition)
      }

      chunk = utils.Chunk(
        params={'ids': ','.join(partition)},
        output=self.output,
        rows_generator=rows_generator(pagination, params, records),
        prefix=f'by_ids/')
      chunks.append(chunk)

    runner = base.Runner(
      chunks_generator=chunks,
      row_to_futures=self.handle,
      total_records=len(ids),
      logger=logger,
      max_workers=6,
    )
    runner.run()

  def run(self):
    total_records = self.count(params=self._get_query_params(
      dataIni=self.params['start_date'].strftime(DATE_FORMAT),
      dataFim=self.params['end_date'].strftime(DATE_FORMAT),
      docsPagina=10
    ))

    # range of ids cause some might be deleted changed on their base
    self.referendaries = self._get_referendaries()
    max_referendary_id   = max([int(c) for c in self.referendaries])
    self.referendaries = [str(cls) for cls in range(1, max_referendary_id + 1)]

    # same for organizations
    self.orgs     = self._get_orgs()
    max_orgs_id   = max([int(c) for c in self.orgs])
    self.orgs     = [str(cls) for cls in range(1, max_orgs_id + 1)]

    # classes as well
    self.classes  = self._get_classes()
    max_cls_id    = max([int(c) for c in self.classes])
    self.classes  = [str(cls) for cls in range(1, max_cls_id + 10)]

    total_filtered = sum([
      self.count(params=self._get_query_params(
        arrorgaos=org,
        dataIni=self.params['start_date'].strftime(DATE_FORMAT),
        dataFim=self.params['end_date'].strftime(DATE_FORMAT),
        docsPagina=10
      )) for org in self.orgs
    ])

    if total_records != total_filtered:
      with sentry_sdk.push_scope() as scope:
        scope.set_extra('crawler', 'TRF4')
        scope.set_extra('total_unfiltered', total_records)
        scope.set_extra('total_filtered'  , total_filtered)
        scope.set_extra('params', self.params)
        sentry_sdk.capture_message('Count does not match', 'warning')
        logger.warn(
          f'Count does not match. Expecting {total_records} got {total_filtered}.')

    runner = base.Runner(
      chunks_generator=self.chunks(),
      row_to_futures=self.handle,
      total_records=total_records,
      logger=logger,
      max_workers=6,
    )
    runner.run()

  def handle(self, events):
    from app.crawlers.tasks import download_from_url

    for event in events:
      if isinstance(event, base.Content):
        args = {
          'filepath': event.dest,
          'contents': event.content,
          'content_type': event.content_type,
          'mode': 'w',
        }
        yield self.output.save_from_contents, args
      elif isinstance(event, base.ContentFromURL):
        yield download_from_url, {
          'url': event.src,
          'dest': event.dest,
          'write_mode': 'wb',
          'content_type': event.content_type,
          'output_uri': self.output.uri
        }
      else:
        raise Exception('Unable to handle ', event)

  @utils.retryable(max_retries=9)  # type: ignore
  def count(self, params):
    response = self.requester.post('https://jurisprudencia.trf4.jus.br/pesquisa/resultado_pesquisa.php',
      data=params, headers=self.header_generator.generate(), timeout=60)
    return self._extract_count_from_text(response.text)

  @utils.retryable(max_retries=9)  # type: ignore
  def _vet_pagination(self, params):
    response = self.requester.post('https://jurisprudencia.trf4.jus.br/pesquisa/resultado_pesquisa.php',
      data=params, headers=self.header_generator.generate(), timeout=60)
    soup = utils.soup_by_content(response.text)
    vet_pagination = soup.find('input', {'type': 'hidden', 'id': 'vetPaginacao'})
    return {
      'total': self._extract_count_from_text(response.text),
      'vetPaginacao': (vet_pagination['value'] if vet_pagination else '')
    }

  def chunks(self):
    ranges = list(utils.timely(
      self.params['start_date'], self.params['end_date'], unit='weeks', step=2))

    for start_date, end_date in reversed(ranges):
      for org in self.orgs:
        chunk_params = {
          'start_date'  : start_date.to_date_string(),
          'end_date'    : end_date.to_date_string(),
          'org'         : org,
        }
        rows_generator =\
          self.rows(start_date=start_date, end_date=end_date, arrorgaos=org)
        yield utils.Chunk(
          params=chunk_params,
          output=self.output,
          rows_generator=rows_generator,
          prefix=f'{start_date.year}/{end_date.month:02d}/')

        time.sleep(.1)
      time.sleep(.5)

  def rows(self, start_date, end_date, **filters):
    params_list = self._find_optimal_filters(start_date, end_date, **filters)

    for params in params_list:
      for row in self._rows_from_params(params):
        yield row

  def _rows_from_params(self, params, page_size=200):
    pagination = self._vet_pagination({**params, **{'docsPagina': page_size}})
    return self._rows_from_pagination(pagination, params, page_size)

  def _rows_from_pagination(self, pagination, params, page_size=200):
    if pagination['total'] == 0:
      return []

    expected_docs_per_page = pagination['vetPaginacao'].split("#")
    rows = []

    for page in range(len(expected_docs_per_page)):
      req_params = {**params, **{
        'checkTabela': 'true',
        'selEscolhaPagina': page,
        'pesquisaLivre': '',
        'registrosSelecionados': '',
        'vetPaginacao': pagination['vetPaginacao'],
        'paginaAtual': page,
        'totalRegistros': pagination['total'],
        'rdoCampoPesquisa': 'I',
        'chkAcordaos': 'on',
        'chkDecMono': '',
        'textoPesqLivre': '',
        'chkDocumentosSelecionados': '',
        'numProcesso': '',
        'tipodata': 'DEC',
        'docsPagina': '50',
        'arrorgaos': '',
        'arrclasses': '',
        'hdnAcao': '',
        'hdnTipo': 1
      }}
      text = self._make_request(req_params)

      if 'Não foram encontrados registros com estes critérios de pesquisa.' in text:
        continue

      # We know exactly which records we will get
      doc_ids = expected_docs_per_page[page].split(',')
      expects = len(doc_ids)
      rows.extend(self._extract_rows(text, expects=expects, params=params))

    assert len(rows) == pagination['total'], \
      f"got {len(rows)} was expecting {pagination['total']}"
    return rows

  @utils.retryable(max_retries=3)
  def _make_request(self, params):
    response = self.requester.post('https://jurisprudencia.trf4.jus.br/pesquisa/resultado_pesquisa.php',
      data=params, headers=self.header_generator.generate(), timeout=60)

    if response.status_code != 200:
      logger.warn(f'Got {response.status_code} for {params}')
    return response.text

  def _extract_rows(self, text, expects, params):
    soup = utils.soup_by_content(text)

    # Collect'em
    tables = soup.find_all('table', {'class': 'tab_resultado'})
    docs = defaultdict(list)
    docs_judgment_date = {}
    docs_urls = {}
    docs_ids  = {}

    doc_index = None

    for tr in tables[0].find_all('tr', recursive=False):
      tds = tr.find_all('td', recursive=False)

      # Doc data has started
      if len(tds) == 1:
        doc_index = tds[0].get_text()
        assert int(doc_index)  # Must be a number

      # Are we reading a doc and it looks like a data table row?
      if len(tds) == 2 and doc_index is not None:
        col_label = tds[0]
        col_value = tds[1]

        # Figure out judgment date -- will use it as date ref.
        if 'Inteiro Teor:' in col_value.get_text() and \
            col_label.get_text() in ['Acórdão', 'DecisãoMonocrática']:
          judgment_date_match =\
            re.match(r'.*Data da Decisão: (\d{2}/\d{2}/\d{4}).*', col_value.get_text(),
                    re.MULTILINE | re.DOTALL)
          if judgment_date_match:
            assert docs_judgment_date.get(doc_index) is None
            docs_judgment_date[doc_index] = judgment_date_match.group(1)

        # Will collect tds
        docs[doc_index].extend(tds)

        # Figure out doc ids
        citation_links = col_value.find_all('img', {'title': 'Visualizar Citação'})
        if len(citation_links) == 1:
          doc_id = re.match(r".*\('(.*)'\).*", citation_links[0]['onclick']).group(1)
          docs_ids[doc_index] = doc_id

        for link in col_value.find_all('a'):
          if 'inteiro_teor.php' in link['href']:
            docs_urls[doc_index] = link['href']

    assert expects == len(docs.keys()), f'{expects} == {len(docs.keys())}'

    for key, cols in docs.items():
      doc_date = docs_judgment_date[key]
      doc_url  = docs_urls[key]
      doc_id   = docs_ids[key]
      assert doc_url
      assert doc_date
      assert doc_id

      _, month, year = doc_date.split('/')
      base_path = f'{year}/{month}'
      doc_html = '\n'.join([col.prettify() for col in cols])
      yield [
        base.Content(content=doc_html,
                     dest=f'{base_path}/{doc_id}_row.html', content_type='text/html'),
        # base.Content(content=doc_url,
        #   dest=f'{base_path}/{doc_id}_url.txt', content_type='text/plain')
        base.ContentFromURL(src=doc_url,
                            dest=f'{base_path}/{doc_id}_INTEIRO.html', content_type='text/html')
      ]

  def _find_optimal_filters(self, start_date, end_date, **filters):
    # This site limits the number of records to 1000.
    # We must make sure it won't surpass this limit.
    default_params = self._get_query_params(
      dataIni=start_date.strftime(DATE_FORMAT),
      dataFim=end_date.strftime(DATE_FORMAT),
      docsPagina=10,
      **filters)
    count = self.count(default_params)
    if count == 0:
      return []

    # Ouch! Exceeded the 1000 records limit.
    if count > 1000:
      logger.warn(
        f'More than 1000 records found on a query (got {count}) -- finding for optimal ranges.')

      all_params = []

      # Reduce the range to the minimal possible.
      params_list = [
        self._get_query_params(
          dataIni=start.strftime(DATE_FORMAT),
          dataFim=end.strftime(DATE_FORMAT),
          docsPagina=10,
          **filters)
        for start, end in utils.timely(start_date, end_date, unit='days', step=1)
      ]
      counts_by_params = [(params, self.count(params)) for params in params_list]

      # Fallback to referendaries first
      # Assume they work better
      for params, count in counts_by_params:
        if count > 1000:
          # Firstly -- get by referendary and for those results
          # where the count still greater than 1000s records...
          logger.debug(f"{count} records. Filtering by referendary based on {params}.")
          counts_by_params_by_referendary =\
              self._find_optimal_params_by_referendaries(params)

          # Filter again by classes only when needed
          for params_by_referendary, count in counts_by_params_by_referendary:
            if count > 1000:
              logger.debug(f"{count} records. Filtering by classes based on {params_by_referendary}.")
              counts_by_params_by_classes = self._find_optimal_params_by_classes(params_by_referendary)
              all_params.extend([params for params, _ in counts_by_params_by_classes])
            elif count > 0:
              logger.debug(f"{count} records. Ok for referendary.")
              all_params.append(params_by_referendary)

        elif count > 0:
          logger.debug(f"{count} records. Ok for range.")
          all_params.append(params)

      return all_params
    else:
      return [default_params]

  def _find_optimal_params_by_referendaries(self, params):
    params_list = [
      {**self._get_query_params(**params), **{'cboRelator': referendary}}
      for referendary in self.referendaries
    ]
    counts_by_params = [(params, self.count(params)) for params in params_list]
    return counts_by_params

  def _find_optimal_params_by_classes(self, params):
    arr = self.classes
    div = 64
    spl = div

    while True:
      n           = math.ceil(len(arr) / div)
      arr         = random.sample(arr, len(arr))
      partitions  = [arr[i:i + n] for i in range(0, len(arr), n)]
      logger.info(f'Partitions {len(partitions)} of {len(arr)}.')
      params_list = [
        {**self._get_query_params(**params), **{'arrclasses': ','.join(partition)}}
        for partition in partitions
      ]

      counts_by_params = [(params, self.count(params)) for params in params_list]
      offenders = [(params, self.count(params)) for count in counts_by_params if count[-1] > 1000]
      logger.info(f'Partitions > 1000 records: {offenders}.')

      if all([count[-1] <= 1000 for count in counts_by_params]):
        return counts_by_params

      div *= 2
      if spl == len(arr):
        break
      spl = min(div, len(arr))
      n = math.ceil(len(arr) / spl)

    with sentry_sdk.push_scope() as scope:
      scope.set_extra('crawler', 'TRF4')
      scope.set_extra('params', params)
      sentry_sdk.capture_message('Impossible situation found -- ignoring', 'warning')
      logger.warn(
        f'Impossible situation found for params {params}.')

  def _get_referendaries(self):
    response = self.requester.get(f'{BASE_URL}/pesquisa.php?tipo=4')
    options = utils.soup_by_content(response.text) \
      .find('select', {'id': 'cboRelator'}).find_all('option')
    return [option['value'] for option in options if len(option['value']) > 0]

  def _get_classes(self):
    classtypes = list(range(1, 5))
    values = []
    for classtype in classtypes:
      response = self.requester.get(f'{BASE_URL}/listar_classes.php?tipo={classtype}')
      options = utils.soup_by_content(response.text) \
        .find_all('input', {'type': 'checkbox', 'class': 'checkbox_sem_fundo'})
      values.extend(
        [option['value'] for option in options if len(option['value']) > 0]
      )
    return list(set(values))

  def _get_orgs(self):
    classtypes = list(range(1, 5))
    values = []
    for classtype in classtypes:
      response = self.requester.get(f'{BASE_URL}/listar_orgaos.php?tipo={classtype}')
      options = utils.soup_by_content(response.text) \
        .find_all('input', {'type': 'checkbox', 'class': 'checkbox_sem_fundo'})
      values.extend(
        [option['value'] for option in options if len(option['value']) > 0]
      )
    return list(set(values))

  def _get_query_params(self, **overrides):
    return {**{
      'rdoTipo': 1,
      'rdoCampoPesquisa': 'I',
      'textoPesqLivre': '',
      'chkAcordaos': 'on',
      'numProcesso': '',
      'cboRelator': '',
      'dataIni': '',
      'dataFim': '',
      'tipodata': 'DEC',
      'docsPagina': '1000',
      'hdnAcao': 'nova_pesquisa',
      'arrclasses': '',
      'arrorgaos': '',
    }, **overrides}

  def _extract_count_from_text(self, text):
    count_txt = re.match(r'.*Foram encontrados (\d+) registros.*', text, re.MULTILINE | re.DOTALL)
    if not count_txt:  # Fallback -- when we've got few results.
      count_txt = re.match(r'.*Documentos encontrados: (\d+).*', text, re.MULTILINE | re.DOTALL)
    return int(count_txt.group(1)) if count_txt else 0


@celery.task(name='crawlers.trf4', default_retry_delay=5 * 60,
             autoretry_for=(BaseException,),
             base=Singleton)
def trf4_task(start_date, end_date, output_uri):
  from app.crawlers.logutils import logging_context

  with logging_context(crawler='trf4'):
    output = utils.get_output_strategy_by_path(path=output_uri)
    logger.info(f'Output: {output}.')
    setup_cloud_logger(logger)

    start_date, end_date =\
      pendulum.parse(start_date), pendulum.parse(end_date)

    crawler = TRF4(params={
      'start_date': start_date, 'end_date': end_date
    }, output=output, logger=logger)
    crawler.run()


@cli.command(name='trf4')
@click.option('--start-date',
  default=utils.DefaultDates.THREE_MONTHS_BACK.strftime("%Y-%m-%d"),
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
def trf4_command(start_date, end_date, output_uri, enqueue, split_tasks):
  args = (start_date, end_date, output_uri)
  if enqueue:
    if split_tasks:
      start_date, end_date =\
        pendulum.parse(start_date), pendulum.parse(end_date)
      for start, end in utils.timely(start_date, end_date, unit=split_tasks, step=1):
        task_id = trf4_task.delay(
          start.to_date_string(),
          end.to_date_string(),
          output_uri)
        print(f"task {task_id} sent with params {start.to_date_string()} {end.to_date_string()}")
    else:
      trf4_task.delay(*args)
  else:
    trf4_task(*args)


@cli.command(name='trf4-seq')
@click.option('--start',
  default=utils.DefaultDates.THREE_MONTHS_BACK.strftime("%Y-%m-%d"),
  help='Format YYYY-MM-DD.',
)
@click.option('--end'  ,
  default=utils.DefaultDates.NOW.strftime("%Y-%m-%d"),
  help='Format YYYY-MM-DD.',
)
@click.option('--output-uri', default=None,  help='Output URI (e.g. gs://bucket_name')
def trf4_seq_command(start, end, output_uri):
  output = utils.get_output_strategy_by_path(path=output_uri)
  logger.info(f'Output: {output}.')

  crawler = TRF4(params={}, output=output, logger=logger)
  crawler.run_for_seq(start, end)