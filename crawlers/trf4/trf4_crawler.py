import re
import time
import utils
import requests
import pendulum
from collections import defaultdict

import base
import click
from app import cli, celery
from logconfig import logger_factory, setup_cloud_logger

logger = logger_factory('trf4')


BASE_URL = 'https://jurisprudencia.trf4.jus.br/pesquisa'


class TRF4(base.BaseCrawler):
  def __init__(self, params, output, logger, **options):
    super(TRF4, self).__init__(params, output, logger, **options)
    self.requester = requests.Session()
    self.header_generator = utils.HeaderGenerator(
      origin='https://jurisprudencia.trf4.jus.br', xhr=False)

  def run(self):
    total_records = self.count()
    runner = base.Runner(
      chunks_generator=self.chunks(),
      row_to_futures=self.handle,
      total_records=total_records,
      logger=logger,
      max_workers=6,
    )
    runner.run()

  def handle(self, events):
    from tasks import download_from_url

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

  def count(self):
    params = self._get_query_params(
      dataIni=self.params['start_date'].strftime('%d/%m/%Y'),
      dataFim=self.params['end_date'].strftime('%d/%m/%Y'),
      docsPagina=10
    )
    response = self.requester.post('https://jurisprudencia.trf4.jus.br/pesquisa/resultado_pesquisa.php',
      data=params, headers=self.header_generator.generate())
    return self._extract_count_from_text(response.text)

  def chunks(self):
    ranges = list(utils.timely(
      self.params['start_date'], self.params['end_date'], unit='months', step=1))

    referendaries = self._get_referendaries()

    for start_date, end_date in reversed(ranges):
      for referendary in referendaries:
        chunk_params = {
          'start_date'  : start_date.to_date_string(),
          'end_date'    : end_date.to_date_string(),
          'referendary' : referendary,
        }
        rows_generator =\
          self.rows(start_date=start_date, end_date=end_date, referendary=referendary)
        yield utils.Chunk(
          params=chunk_params,
          output=self.output,
          rows_generator=rows_generator,
          prefix=f'{start_date.year}/{end_date.month:02d}/')

        time.sleep(.1)
      time.sleep(.5)

  def rows(self, start_date, end_date, referendary):
    optimal_ranges = self._find_optimal_ranges(start_date, end_date, referendary)

    for opt_start_date, opt_end_date in optimal_ranges:
      params = self._get_query_params(
        cboRelator=referendary,
        dataIni=opt_start_date.strftime('%d/%m/%Y'),
        dataFim=opt_end_date.strftime('%d/%m/%Y'),
      )
      response = self.requester.post('https://jurisprudencia.trf4.jus.br/pesquisa/resultado_pesquisa.php',
        data=params, headers=self.header_generator.generate())

      assert response.status_code == 200, \
        f'Got {response.status_code} params {params}'
      if 'Não foram encontrados registros com estes critérios de pesquisa.' in response.text:
        continue

      # Assert number of docs
      num_of_docs = self._extract_count_from_text(response.text)
      assert num_of_docs <= 1000
      for row in self._extract_rows(response.text, expects=num_of_docs, params=params):
        yield row

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
        col_value = tds[1]

        # Figure out judgment date -- will use it as date ref.
        if 'Inteiro Teor:' in col_value.get_text():
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
          docs_ids[doc_index] =\
            re.match(r".*\('(.*)'\).*", citation_links[0]['onclick']).group(1)

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
          dest=f'{base_path}/{doc_id}_row.html'   , content_type='text/html'),
        base.ContentFromURL(src=doc_url,
          dest=f'{base_path}/{doc_id}_report.html', content_type='text/html')
      ]

    next_btn = soup.find_all('input', {'id': 'sbmProximaPagina'})
    assert len(next_btn) == 0, 'No next button'

  def _find_optimal_ranges(self, start_date, end_date, referendary):
    import math
    def _count(start, end):
      params = self._get_query_params(
        cboRelator=referendary,
        dataIni=start.strftime('%d/%m/%Y'),
        dataFim=end.strftime('%d/%m/%Y'),
        docsPagina=10
      )
      response = self.requester.post('https://jurisprudencia.trf4.jus.br/pesquisa/resultado_pesquisa.php',
        data=params, headers=self.header_generator.generate())
      return self._extract_count_from_text(response.text)

    # This site limits the number of records to 1000.
    # We must make sure it won't surpass this limit.
    num_of_docs = _count(start_date, end_date)
    if num_of_docs == 0:
      return []

    if num_of_docs > 1000:
      logger.warn(
        f'More than 1000 records found on a query (got {num_of_docs}) -- finding for optimal ranges.')
      days  = start_date.diff(end_date).in_days()
      step  = math.ceil(days / 2)
      while True:
        ranges = list(utils.timely(start_date, end_date, unit='days', step=step))
        if all([_count(start, end) <= 1000 for start, end in ranges]):
          return ranges
        if step == 1:
          break
        step = math.ceil(step / 2)
      raise Exception(
        f'Unable to find optional ranges for {start_date}, {end_date} and {referendary}.')
    else:
      yield [start_date, end_date]

  def _get_referendaries(self):
    response = self.requester.get(f'{BASE_URL}/pesquisa.php?tipo=4')
    options = utils.soup_by_content(response.text) \
      .find('select', {'id': 'cboRelator'}).find_all('option')
    return [option['value'] for option in options if len(option['value']) > 0]

  def _get_query_params(self, **overrides):
    return {**{
      'rdoTipo': 4,
      'rdoCampoPesquisa': 'I',
      'textoPesqLivre': '',
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


@celery.task(queue='crawlers.trf4', default_retry_delay=5 * 60,
             autoretry_for=(BaseException,))
def trf4_task(start_date, end_date, output_uri):
  from logutils import logging_context

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
@click.option('--start-date', prompt=True,   help='Format YYYY-MM-DD.')
@click.option('--end-date'  , prompt=True,   help='Format YYYY-MM-DD.')
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