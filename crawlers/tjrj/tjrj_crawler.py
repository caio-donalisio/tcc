import time
import random
import requests
import utils
import json
import datetime
import re

from celery_singleton import Singleton

import base
import click
import browsers
from app import cli, celery
from logconfig import logger_factory, setup_cloud_logger

logger = logger_factory('tjrj')


BASE_URL = 'http://www4.tjrj.jus.br'
EJURIS_URL = f'{BASE_URL}/EJURIS'
EJUD_URL = f'{BASE_URL}/ejud/WS'
GED_URL = f'http://www1.tjrj.jus.br/gedcacheweb'
QUERY = 'ação'
DATE_FORMAT = "%d/%m/%Y"


def decode_epoch_time(string):
    timestamp = re.split('\(|\)', string)[1][:10]
    time = datetime.datetime.fromtimestamp(int(timestamp))
    return time.strftime(DATE_FORMAT)


class TJRJ(base.BaseCrawler):
  def __init__(self, params, output, logger, browser, **options):
    super(TJRJ, self).__init__(params, output, logger, **options)
    self.browser   = browser
    self.requester = requests.Session()

  def run(self):
    try:
      self.session_id  = self._get_session_id()
    finally:
      self.browser.quit()

    headers = {
      "cookie": f'ASP.NET_SessionId={self.session_id}',
      "Content-Type": "application/json"
    }
    payload = {
      'pageSeq': '0',
      'grpEssj': ''
    }

    import math
    total_records = self.count(payload, headers)
    total_pages   = math.ceil(total_records / 10.)

    runner = base.Runner(
      chunks_generator=self.chunks(total_pages),
      row_to_futures=self.handle,
      total_records=total_records,
      logger=logger,
    )

    runner.run()

  def handle(self, events):
    for event in events:
      if isinstance(event, base.Content):
        args = {
          'filepath': event.dest,
          'contents': event.content,
          'content_type': event.content_type
        }
        yield self.output.save_from_contents, args
      elif isinstance(event, base.ContentFromURL):
        yield self.download_from_url, {
          'content_from_url': event
        }
      else:
        raise Exception('Unable to handle ', event)

  @utils.retryable(max_retries=3, sleeptime=5., ignore_if_exceeds=True)   # type: ignore
  def download_from_url(self, content_from_url):
    if self.output.exists(content_from_url.dest):
      return

    response = self.requester.get(content_from_url.src,
      allow_redirects=True,
      verify=False,
      timeout=10)

    if content_from_url.content_type == 'text/html':
      if response.status_code == 200:
        self.output.save_from_contents(
          filepath=content_from_url.dest,
          contents=response.content,
          content_type=content_from_url.content_type)
      else:
        logger.warn(
          f"Got a wait page when fetching {content_from_url.src} -- will retry.")
        raise utils.PleaseRetryException()

    # For TJRJ we usually downloads some pdfs but as well as some htmls.
    # Sometimes they return a html page when we are downloading the pdf... We have to deal with this.
    if content_from_url.content_type == 'application/pdf':
      if 'application/pdf' in response.headers.get('Content-type') or \
          'application/x-zip-compressed' in response.headers.get('Content-type'):
        self.output.save_from_contents(
          filepath=content_from_url.dest,
          contents=response.content,
          content_type='application/pdf')
      else:
        if response.status_code == 200 and \
          'text/html' in response.headers.get('Content-type'):
          # Got a warning message -- Shuld we retry?
          logger.warn(
            f"Got a wait page when fetching {content_from_url.src} -- will retry.")
          raise utils.PleaseRetryException()

        logger.warn(
          f"Got {response.status_code} when fetching {content_from_url.src}. Content-type: {response.headers.get('Content-type')}.")

  def chunks(self, number_of_pages):
    for page in range(1, number_of_pages + 1):
      chunk_params = {
        'start_year': self.params['start_year'],
        'end_year'  : self.params['end_year'],
        'page'      : page,
      }

      yield utils.Chunk(
        params=chunk_params,
        output=self.output,
        rows_generator=self.rows(page=page),
        prefix=f"{self.params['start_year']}/")

  def rows(self, page):
    headers = {
      "cookie": f'ASP.NET_SessionId={self.session_id}',
      "Content-Type": "application/json"
    }
    payload = {
      'pageSeq': '0',
      'grpEssj': ''
    }

    acts = self._get_acts(page, payload, headers)
    for act in acts:
      act_id     = act['CodDoc']
      updated_at = decode_epoch_time(act['DtHrMov'])
      filepath   = utils.get_filepath(str(updated_at), act_id, 'json')

      extra_contents = []
      fetch_all_pdfs = True

      if act['TemBlobValido']:
        doc_path = utils.get_filepath(date=str(updated_at), filename=f'{act_id}_inteiro_teor', extension='html')
        extra_contents.append(base.ContentFromURL(
          src=f"http://www4.tjrj.jus.br/EJURIS/ExportaInteiroTeor.aspx?CodDoc={act_id}&PageSeq=0&EFT=1",
          dest=doc_path,
          content_type='text/html'
        ))

      # INFO: We might won't download all pdfs now. Regardless `TemBlobValid` is False.
      # What we have in `ExportaInteiroTeor` is what matters, not always available though.
      fetch_all_pdfs = self.options.get('skip_pdf', False) == False

      # fetch extra content as well
      extra_contents.extend(self.fetch_data(act, headers, act_id, updated_at,
        fetch_all_pdfs=fetch_all_pdfs))

      yield [
        base.Content(content=json.dumps(act, ensure_ascii=False), dest=filepath,
                     content_type='application/json'),
        *extra_contents
      ]
      time.sleep(random.uniform(.05, .20))

  @utils.retryable(max_retries=3)
  def fetch_data(self, act, headers, act_id, updated_at, fetch_all_pdfs=False):
    data_url = f'{EJUD_URL}/ConsultaEjud.asmx/DadosProcesso_1'
    data_payload = {
      'nAntigo': act['NumAntigo'],
      'pCPF': '',
      'pLogin': ''
    }
    logger.debug(f"POST {act['NumAntigo']}: {data_url}")
    data_response = self.requester.post(
        data_url, json=data_payload, headers=headers)
    data_result = data_response.json()['d']
    doc_filename = f'{act_id}-data'
    filepath = utils.get_filepath(
        str(updated_at), doc_filename, 'json')

    extra_contents = []

    if fetch_all_pdfs:
      documents = data_result.get('InteiroTeor', [])
      for document in documents:
        gedid = document['ArqGED']
        pdf_url = f'{GED_URL}/default.aspx?GEDID={gedid}'
        pdf_filename = f'{act_id}-{gedid}'
        pdf_filepath = utils.get_filepath(
            date=str(updated_at), filename=pdf_filename, extension='pdf')
        extra_contents.append(base.ContentFromURL(
          src=pdf_url, dest=pdf_filepath, content_type='application/pdf'
        ))

    return [
      base.Content(content=self._dump(data_result), dest=filepath,
                  content_type='application/json'),
      *extra_contents
    ]

  @utils.retryable(max_retries=3)
  def _get_acts(self, page, payload, headers):
    payload['numPagina'] = page
    url = f'{EJURIS_URL}/ProcessarConsJuris.aspx/ExecutarConsultarJurisprudencia'
    logger.debug(f'POST (get_acts) {url}')
    response = self.requester.post(url, json=payload, headers=headers)
    result = response.json()['d']
    return result['DocumentosConsulta']

  @utils.retryable(max_retries=3)
  def count(self, payload, headers):
    payload['numPagina'] = 0
    url = f'{EJURIS_URL}/ProcessarConsJuris.aspx/ExecutarConsultarJurisprudencia'
    logger.debug(f'POST (count) {url}')
    response = self.requester.post(url, json=payload, headers=headers)
    result = response.json()['d']
    return result['TotalDocs']

  def _dump(self, dictionary):
    return json.dumps(dictionary, ensure_ascii=False)

  def _get_session_id(self):
    url = f'{EJURIS_URL}/ConsultarJurisprudencia.aspx'
    logger.debug(f'GET {url}')

    self.browser.get(url)
    self.browser.fill_in(
        field_id='ContentPlaceHolder1_txtTextoPesq', value=QUERY)
    self.browser.select_by_id('ContentPlaceHolder1_cmbAnoInicio', self.params['start_year'])
    self.browser.select_by_id('ContentPlaceHolder1_cmbAnoFim', self.params['end_year'])
    search_button = self._find(id='ContentPlaceHolder1_btnPesquisar')
    self.browser.click(search_button)
    session_id = None
    cookies = self.browser.driver.get_cookies()

    for cookie in cookies:
      if cookie.get('name') == 'ASP.NET_SessionId':
        session_id = cookie['value']
        logger.info(f'Got ASP.NET_SessionId: {session_id}')
    return session_id


@celery.task(queue='crawlers.tjrj', default_retry_delay=5 * 60,
             autoretry_for=(BaseException,),
             base=Singleton)
def tjrj_task(start_year, end_year, output_uri, pdf_async, skip_pdf):
  from logutils import logging_context

  with logging_context(crawler='tjrj'):
    output = utils.get_output_strategy_by_path(path=output_uri)
    logger.info(f'Output: {output}.')
    setup_cloud_logger(logger)

    options = dict(pdf_async=pdf_async, skip_pdf=skip_pdf)

    crawler = TJRJ(params={
      'start_year': start_year, 'end_year': end_year
    }, output=output, logger=logger, browser=browsers.FirefoxBrowser(), **options)
    crawler.run()


@cli.command(name='tjrj')
@click.option('--start-year', prompt=True,   help='Format YYYY.')
@click.option('--end-year'  , prompt=True,   help='Format YYYY.')
@click.option('--output-uri', default=None,  help='Output URI (e.g. gs://bucket_name')
@click.option('--pdf-async' , default=False, help='Download PDFs async'   , is_flag=True)
@click.option('--skip-pdf'  , default=False, help='Skip PDF download'     , is_flag=True)
@click.option('--enqueue'   , default=False, help='Enqueue for a worker'  , is_flag=True)
def tjrj_command(start_year, end_year, output_uri, pdf_async, skip_pdf, enqueue):
  args = (start_year, end_year, output_uri, pdf_async, skip_pdf)
  if enqueue:
    print("task_id", tjrj_task.delay(*args))
  else:
    tjrj_task(*args)