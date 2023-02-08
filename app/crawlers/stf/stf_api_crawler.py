from app.crawlers.stf.stf_api_query import get_query
from app.crawlers.logconfig import logger_factory
from app.celery_run import celery_app as celery
from app.crawler_cli import cli
import click
import json
import time
import requests
import pendulum
from app.crawlers import utils
import logging
import random

from tqdm import tqdm
from urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)


DEFAULT_HEADERS = {
    'Accept': 'application/json, text/plain, */*',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:84.0) Gecko/20100101 Firefox/84.0'
}


class STF:
  def __init__(self, params, output, logger, **options):
    self.params = params
    self.output = output
    self.logger = logger
    self.options = (options or {})

  def run(self):
    total_records = self.count()
    self.logger.info(f'Expects {total_records} records.')
    records_fetch = 0

    tqdm_out = utils.TqdmToLogger(self.logger, level=logging.INFO)
    with tqdm(total=total_records, file=tqdm_out) as pbar:
      for chunk in self.chunks():
        if chunk.commited():
          chunk_records = chunk.get_value('records')
          records_fetch += chunk_records
          pbar.update(chunk_records)
          self.logger.debug(f"Chunk {chunk.hash} already commited ({chunk_records} records) -- skipping.")
          continue

        chunk_records = self.process_chunk(chunk)

        records_fetch += chunk_records
        pbar.update(chunk_records)
        self.logger.debug(f'Chunk {chunk.hash} ({chunk_records} records) commited.')

    self.logger.info(f'Expects {total_records}. Fetched {records_fetch}.')
    # assert total_records <= records_fetch

  def process_chunk(self, chunk):
    chunk_records = 0
    for doc, pdf in chunk.rows():
      self.handle_doc(doc)
      self.handle_pdf(pdf)
      chunk_records += 1

    chunk.set_value('records', chunk_records)
    chunk.commit()
    return chunk_records

  def handle_doc(self, doc):
    self.output.save_from_contents(
        filepath=doc['dest'],
        contents=json.dumps(doc['source']),
        content_type='application/json')

  @utils.retryable(max_retries=3)   # type: ignore
  def handle_pdf(self, pdf):
    from app.crawlers.tasks import download_from_url
    download_args = dict(
        url=pdf['url'], dest=pdf['dest'], output_uri=self.output.uri,
        headers=DEFAULT_HEADERS, content_type='application/pdf', write_mode='wb',
        override=False)
    if self.options.get('skip_pdf', False) is False:
      if self.options.get('pdf_async', False):
        download_from_url.delay(**download_args)
      else:
        download_from_url(**download_args)
        time.sleep(random.uniform(0.1, 0.2))

  def chunks(self):
    ranges = list(utils.timely(
        self.params['start_date'], self.params['end_date'], unit='days', step=3))
    for start_date, end_date in reversed(ranges):
      chunk_params = {
          'start_date': start_date.to_date_string(),
          'end_date': end_date.to_date_string()
      }
      yield utils.Chunk(params=chunk_params, output=self.output,
                        rows_generator=self.rows(start_date=start_date, end_date=end_date))

  def rows(self, start_date, end_date, sleeptime=1., page_size=250):
    total_records = float('inf')
    offset = 0
    while offset < total_records:
      query = get_query(
          start_date=start_date.start_of('day').to_date_string(),
          end_date=end_date.end_of('day').to_date_string(),
          offset=offset,
          size=page_size)
      result =\
          self._perform_search(query)['result']['hits']
      total_records = result['total']['value']
      for hit in result['hits']:
        yield self.get_doc(hit)
      time.sleep(random.uniform(sleeptime, sleeptime * 2.))
      offset += page_size

  def count(self):
    result = self._perform_search(
        query=get_query(
            start_date=self.params['start_date'].to_date_string(),
            end_date=self.params['end_date'].to_date_string(),
            offset=0,
            size=1))
    result = result['result']['hits']
    return result['total']['value']

  def get_doc(self, row):
    import re
    NUMBER_PATTERN = re.compile(r'http.*idDocumento=(\d+)')

    doc = row['_source']
    doc_id = doc['id']
    pdf_url = doc['inteiro_teor_url']

    doc_number = re.search(NUMBER_PATTERN, pdf_url).group(1)
    actual_pdf_url = f"https://portal.stf.jus.br/jurisprudencia/obterInteiroTeor.asp?idDocumento={doc_number}"
    pub_date = pendulum.parse(doc['publicacao_data'])
    month = '{:02d}'.format(pub_date.month)
    return {
        'source': doc,
        'dest': f'{pub_date.year}/{month}/{doc_id}.json',
    }, {
        'url': actual_pdf_url,
        'dest': f'{pub_date.year}/{month}/{doc_id}.pdf'
    }

  @utils.retryable(max_retries=3)   # type: ignore
  def _perform_search(self, query):
    response =\
        requests.post('https://jurisprudencia.stf.jus.br/api/search/search',
                      data=json.dumps(query),
                      headers=DEFAULT_HEADERS,
                      verify=False)
    if response.status_code != 200:
      self.logger.warn(f'Expects 200. Got {response.status_code}.')
      self.logger.warn(response.text)
      raise utils.PleaseRetryException()
    return response.json()


@celery.task(name='crawlers.stf', rate_limit='2/h', default_retry_delay=30 * 60,
             autoretry_for=(Exception,))
def stf_task(start_date, end_date, output_uri, pdf_async, skip_pdf):
  start_date, end_date =\
      pendulum.parse(start_date), pendulum.parse(end_date)

  output = utils.get_output_strategy_by_path(path=output_uri)
  logger = logger_factory('stf')
  logger.info(f'Output: {output}.')

  crawler = STF(params={
      'start_date': start_date, 'end_date': end_date
  }, output=output, logger=logger, pdf_async=pdf_async, skip_pdf=skip_pdf)
  crawler.run()


@cli.command(name='stf')
@click.option('--start-date',
              default=utils.DefaultDates.THREE_MONTHS_BACK.strftime("%Y-%m-%d"),
              help='Format YYYY-MM-DD.',
              )
@click.option('--end-date',
              default=utils.DefaultDates.NOW.strftime("%Y-%m-%d"),
              help='Format YYYY-MM-DD.',
              )
@click.option('--output-uri', default=None,  help='Output URI (e.g. gs://bucket_name')
@click.option('--pdf-async', default=False, help='Download PDFs async', is_flag=True)
@click.option('--skip-pdf', default=False, help='Skip PDF download', is_flag=True)
@click.option('--enqueue', default=False, help='Enqueue for a worker', is_flag=True)
@click.option('--split-tasks',
              default=None, help='Split tasks based on time range (weeks, months, days, etc) (use with --enqueue)')
def stf_command(**kwargs):
  enqueue, split_tasks = kwargs.get('enqueue'), kwargs.get('split_tasks')
  del (kwargs['enqueue'])
  del (kwargs['split_tasks'])
  if enqueue:
    utils.enqueue_tasks(stf_task, split_tasks, **kwargs)
  else:
    stf_task(**kwargs)
