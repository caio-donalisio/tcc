import json
import time
import requests
import pendulum
import utils
import logging
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

import click
from app import cli, celery
from tasks import download_from_url

from crawlers.stf.stf_api_query import get_query

DEFAULT_HEADERS = {
  'Accept': 'application/json, text/plain, */*',
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:84.0) Gecko/20100101 Firefox/84.0'
}


class STF:
  def __init__(self, params, output, logger, pdf_async=False):
    self.params = params
    self.output = output
    self.logger = logger
    self.pdf_async = pdf_async

  def run(self):
    total_records = self.count()
    self.logger.info(f'Expects {total_records} records.')
    records_fetch = 0

    tqdm_out = utils.TqdmToLogger(self.logger, level=logging.INFO)
    with tqdm(total=total_records, file=tqdm_out) as pbar:
      for doc, pdf in self.rows():
        self.output.save_from_contents(
          filepath=doc['dest'],
          contents=json.dumps(doc['source']),
          content_type='application/json')

        download_args = dict(
          url=pdf['url'], dest=pdf['dest'], output_uri=self.output.uri,
          headers=DEFAULT_HEADERS, content_type='application/pdf', write_mode='wb')
        if self.pdf_async:
          download_from_url.delay(**download_args)
        else:
          download_from_url(**download_args)

        records_fetch += 1
        pbar.update(1)

    self.logger.info(f'Expects {total_records}. Fetched {records_fetch}.')
    assert total_records == records_fetch

  def rows(self, sleeptime=1., offset=0, page_size=250):
    total_records = None
    while True:
      result =\
        self._perform_search(offset, page_size)['result']['hits']
      total_records = result['total']['value']
      for hit in result['hits']:
        yield self.get_doc(hit)
      time.sleep(sleeptime)
      offset += page_size
      if offset >= total_records:
        break

  def count(self):
    result = self._perform_search(offset=0, page_size=1)
    result = result['result']['hits']
    return result['total']['value']

  def get_doc(self, row):
    doc      = row['_source']
    doc_id   = doc['id']
    pdf_url  = doc['inteiro_teor_url']
    pub_date = pendulum.parse(doc['publicacao_data'])
    month    = '{:02d}'.format(pub_date.month)
    return {
      'source': doc,
      'dest'  : f'{pub_date.year}/{month}/{doc_id}.json',
    }, {
      'url'   : pdf_url,
      'dest'  : f'{pub_date.year}/{month}/{doc_id}.pdf'
    }

  def _perform_search(self, offset=0, page_size=100):
    query = get_query(
      start_date=self.params['start_date'].to_date_string(),
      end_date=self.params['end_date'].to_date_string(),
      offset=offset,
      size=page_size)

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


@cli.command(name='stf')
@click.option('--start-date', prompt=True, help='Start date (format YYYY-MM-DD).')
@click.option('--end-date'  , prompt=True, help='End date (format YYYY-MM-DD).')
@click.option('--output-uri', default=None, help='Output URI')
@click.option('--pdf-async' , default=False, help='Download PDFs async', is_flag=True)
def stf_command(start_date, end_date, output_uri, pdf_async):
  start_date, end_date =\
    pendulum.parse(start_date), pendulum.parse(end_date)

  logger = utils.setup_logger('stf', 'logs/stf/stf.log')

  output = utils.get_output_strategy_by_path(path=output_uri)
  logger.info(f'Output: {output}.')

  crawler = STF(params={
    'start_date': start_date, 'end_date': end_date
  }, output=output, logger=logger, pdf_async=pdf_async)
  crawler.run()