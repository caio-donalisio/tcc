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
from app import cli

from crawlers.stf.stf_api_query import get_query

DEFAULT_HEADERS = {
  'Accept': 'application/json, text/plain, */*',
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:84.0) Gecko/20100101 Firefox/84.0'
}


class STF:
  def __init__(self, params, output, logger):
    self.params = params
    self.output = output
    self.logger = logger

  def run(self):
    total_records = self.count()
    self.logger.info(f'Expects {total_records} records.')

    records_fetch = 0
    offset, page_size = 0, 250

    tqdm_out = utils.TqdmToLogger(self.logger, level=logging.INFO)
    with tqdm(total=total_records, file=tqdm_out) as pbar:
      while True:
        for row in self.rows(offset=offset, page_size=page_size):
          self.fetch_row(row)
          records_fetch += 1
          pbar.update(1)
        time.sleep(1)
        offset += page_size
        if offset >= total_records:
          break

    self.logger.info(f'Expects {total_records}. Fetched {records_fetch}.')
    assert total_records == records_fetch

  def count(self):
    result = self._perform_search(offset=0, page_size=1)
    result = result['result']['hits']
    return result['total']['value']

  def rows(self, offset=0, page_size=100):
    result = self._perform_search(offset, page_size)
    for row in result['result']['hits']['hits']:
      yield row

  @utils.retryable(max_retries=3)  # type: ignore
  def fetch_row(self, row):
    doc      = row['_source']
    doc_id   = doc['id']
    pdf_url  = doc['inteiro_teor_url']
    pub_date = pendulum.parse(doc['publicacao_data'])

    # Raw json
    month = '{:02d}'.format(pub_date.month)
    json_filepath = f'{pub_date.year}/{month}/{doc_id}.json'
    self.output.save_from_contents(filepath=json_filepath, contents=json.dumps(row['_source']),
      content_type='application/json')

    # PDF
    pdf_filepath = f'{pub_date.year}/{month}/{doc_id}.pdf'
    response = requests.get(pdf_url, allow_redirects=True, verify=False,
      headers=DEFAULT_HEADERS)
    self.output.save_from_contents(
      filepath=pdf_filepath, contents=response.content, mode='wb',
      content_type='application/pdf')

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
def stf_command(start_date, end_date, output_uri):
  start_date, end_date =\
    pendulum.parse(start_date), pendulum.parse(end_date)

  logger = utils.setup_logger('stf', 'logs/stf/stf.log')

  output = utils.get_output_strategy_by_path(path=output_uri)
  logger.info(f'Output: {output}.')

  crawler = STF(params={
    'start_date': start_date, 'end_date': end_date
  }, output=output, logger=logger)
  crawler.run()