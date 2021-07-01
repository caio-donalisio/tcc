import json
import time
import requests
import pendulum
import utils
import random
import logging
from tqdm import tqdm

from urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

import click
from app import cli, celery

class TRF2:
  def __init__(self, params, output, logger, **options):
    self.params = params
    self.output = output
    self.logger = logger
    self.options = (options or {})
    self.header_generator = utils.HeaderGenerator(
      origin='https://www10.trf2.jus.br', xhr=True)

  def run(self):
    records_fetch = 0

    tqdm_out = utils.TqdmToLogger(self.logger, level=logging.INFO)
    with tqdm(file=tqdm_out) as pbar:
      for chunk in self.chunks():
        if chunk.commited():
          chunk_records  = chunk.get_value('records')
          records_fetch += chunk_records
          pbar.update(chunk_records)
          self.logger.info(f"Chunk {chunk.hash} already commited ({chunk_records} records) -- skipping.")
          continue

        chunk_records = 0
        for doc, pdf in chunk.rows():
          self.handle_doc(doc)
          self.handle_pdf(pdf)
          chunk_records += 1

        chunk.set_value('records', chunk_records)
        chunk.commit()
        records_fetch += chunk_records
        pbar.update(chunk_records)
        self.logger.debug(f'Chunk {chunk.hash} ({chunk_records} records) commited')

    self.logger.info(f'Fetched {records_fetch}.')

  def handle_doc(self, doc):
    self.output.save_from_contents(
      filepath=doc['dest'],
      contents=json.dumps(doc['source']),
      content_type='text/html')

  def handle_pdf(self, pdf):
    from tasks import download_from_url
    if pdf['url'] is None:
      return

    download_args = dict(
      url=pdf['url'],
      dest=pdf['dest'],
      output_uri=self.output.uri,
      headers=self.header_generator.generate(),
      content_type='application/pdf',
      write_mode='wb',
      override=False)

    if self.options.get('skip_pdf', False) is False:
      if self.options.get('pdf_async', False):
        download_from_url.delay(**download_args)
      else:
        download_from_url(**download_args)
        time.sleep(random.uniform(1, 2))

  def chunks(self):
    for start_date, end_date in \
      utils.timely(self.params['start_date'], self.params['end_date'], unit='days', step=1):
      chunk_params = {
        'start_date': start_date.to_date_string(),
        'end_date'  : end_date.to_date_string()
      }
      yield utils.Chunk(params=chunk_params, output=self.output,
        rows_generator=self.rows(start_date=start_date, end_date=end_date))

  def rows(self, start_date, end_date):
    offset = 0
    while True:
      query = {
        'start_date': start_date.start_of('day').to_date_string(),
        'end_date'  : end_date.start_of('day').to_date_string(),
        'offset'    : offset,
      }
      html = self._perform_search(query)
      soup = utils.soup_by_content(html)
      uls  = soup.find_all('ul', {'class': 'ul-resultados'})
      if len(uls) == 0:
        break
      assert len(uls) == 1

      # figure out last page
      last_page_link = soup.find_all('a', {'class': 'pagination-link'}, string='Último')
      last_page = len(last_page_link) == 0

      rows = uls[0].find_all('li', recursive=False)
      offset += len(rows)

      for row in rows:
        links = row.find_all('a', {'class': 'font_bold'})
        pdf_url  = None
        doc, pdf = None, None

        for link in links:
          # pdf (available on listing)
          if link.get_text() == 'Inteiro teor':
            pdf_url = link['href']

          # content
          if link.get_text() == 'Pré-visualização':
            preview_path = link['href']
            url = f'https://www10.trf2.jus.br/consultas/{preview_path}'
            doc, pdf = self.fetch_doc(url=url)

        # Pdf wasn't available on html -- use what we got from listing.
        if pdf['url'] is None:
          pdf['url'] = pdf_url

        yield doc, pdf

      if last_page:
        break
      time.sleep(random.uniform(2, 5))

  @utils.retryable(max_retries=3)   # type: ignore
  def fetch_doc(self, url):
    from urllib.parse import parse_qs, urlsplit
    doc_info = parse_qs(
      parse_qs(urlsplit(url).query)['q'][0].split('?')[-1])

    filename       = f"{doc_info['processo'][0]}_{doc_info['coddoc'][0]}"
    year, month, _ = doc_info['datapublic'][0].split('-')

    response = requests.get(url, allow_redirects=True, verify=False, timeout=3)

    pdf_url = None
    for link in utils.soup_by_content(response.text).find_all('a'):
      if 'inteiro teor' in link.get_text().lower():
        pdf_url = link['href']

    return {
      'source': response.text,  # whole content
      'dest'  : f'{year}/{month}/{filename}.html',
    }, {
      'url'   : pdf_url,
      'dest'  : f'{year}/{month}/{filename}.pdf'
    }

  @utils.retryable(max_retries=3)   # type: ignore
  def _perform_search(self, query):
    query_url = 'https://www10.trf2.jus.br/consultas/?proxystylesheet=v2_index&getfields=*&entqr=3&lr=lang_pt&ie=UTF-8&oe=UTF-8&requiredfields=(-sin_proces_sigilo_judici:s).(-sin_sigilo_judici:s)&sort=date:A:S:d1&entsp=a&adv=1&base=JP-TRF&ulang=&access=p&entqrm=0&wc=200&wc_mc=0&ud=1&client=v2_index&filter=0&as_q=inmeta:DataDispo:daterange:{start_date}..{end_date}&q=+inmeta:gsaentity_BASE%3DInteiro%2520Teor&start={offset}&num=1&site=v2_jurisprudencia'.format(
      start_date=query['start_date'], end_date=query['end_date'], offset=query['offset'])

    response = requests.post(
      query_url,
      headers=self.header_generator.generate(),
      verify=False,
      timeout=3)

    if response.status_code != 200:
      self.logger.warn(f'Expects 200. Got {response.status_code}.')
      self.logger.warn(response.text)
      raise utils.PleaseRetryException()
    return response.text


@celery.task(queue='crawlers', rate_limit='1/h')
def trf2_task(start_date, end_date, output_uri, pdf_async, skip_pdf):
  start_date, end_date =\
    pendulum.parse(start_date), pendulum.parse(end_date)

  logger = utils.setup_logger('trf2', 'logs/trf2/trf2.log')

  output = utils.get_output_strategy_by_path(path=output_uri)
  logger.info(f'Output: {output}.')

  crawler = TRF2(params={
    'start_date': start_date, 'end_date': end_date
  }, output=output, logger=logger, pdf_async=pdf_async, skip_pdf=skip_pdf)
  crawler.run()


@cli.command(name='trf2')
@click.option('--start-date', prompt=True,   help='Format YYYY-MM-DD.')
@click.option('--end-date'  , prompt=True,   help='Format YYYY-MM-DD.')
@click.option('--output-uri', default=None,  help='Output URI (e.g. gs://bucket_name')
@click.option('--pdf-async' , default=False, help='Download PDFs async'   , is_flag=True)
@click.option('--skip-pdf'  , default=False, help='Skip PDF download'     , is_flag=True)
@click.option('--enqueue'   , default=False, help='Enqueue for a worker'  , is_flag=True)
def trf2_command(start_date, end_date, output_uri, pdf_async, skip_pdf, enqueue):
  args = (start_date, end_date, output_uri, pdf_async, skip_pdf)
  if enqueue:
    trf2_task.delay(*args)
  else:
    trf2_task(*args)
