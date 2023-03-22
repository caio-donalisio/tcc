from app.crawlers import base, utils, browsers
import math
import json
import pendulum
from app.crawlers.logconfig import logger_factory, setup_cloud_logger
import click
from app.celery_run import celery_app as celery
from app.crawler_cli import cli
import requests
import re
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

logger = logger_factory('carf')
NOW = pendulum.now()
MIN_FILE_SIZE=100  # TODO: Add PDF collection logic into content handler
DEFAULT_PDF_HEADERS = {
      'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:108.0) Gecko/20100101 Firefox/108.0',
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
      'Accept-Language': 'en-US,en;q=0.5',
      # 'Accept-Encoding': 'gzip, deflate, br',
      'Referer': 'https://carf.fazenda.gov.br/sincon/public/pages/ConsultarJurisprudencia/listaJurisprudenciaCarf.jsf',
      'Content-Type': 'application/x-www-form-urlencoded',
      'Origin': 'https://carf.fazenda.gov.br',
      'Connection': 'keep-alive',
      # 'Cookie': 'ss_lbappsincon=1.vv5993; JSESSIONID=BA771EBF7C26F3BB57BEAA0FE71996A9',
      'Upgrade-Insecure-Requests': '1',
      'Sec-Fetch-Dest': 'document',
      'Sec-Fetch-Mode': 'navigate',
      'Sec-Fetch-Site': 'same-origin',
      'Sec-Fetch-User': '?1',
      }

class CARFClient:

  def __init__(self):
    self.url = 'https://acordaos.economia.gov.br/solr/acordaos2/browse?'

  @utils.retryable(max_retries=3)
  def count(self, filters):
    result = self.fetch(filters, page=1)
    return result['response']['numFound']

  @utils.retryable(max_retries=3)
  def fetch(self, filters, page=1):

    items_per_page = filters.get('rows')

    params = {
        **filters,
        **{'start': (page - 1) * items_per_page}
    }

    return requests.get(self.url,
                        params=params,
                        verify=False).json()


class CARFCollector(base.ICollector):

  def __init__(self, client, filters):
    self.client = client
    self.filters = filters

  def count(self):
    return self.client.count(self.filters)

  def chunks(self):
    total = self.count()
    pages = math.ceil(total/self.filters.get('rows'))

    for page in range(1, pages + 1):
      yield CARFChunk(
          keys={
              **self.filters, **{'page': page, 'count': total,}
          },
          prefix='',
          filters=self.filters,
          page=page,
          
          client=self.client

      )


class CARFChunk(base.Chunk):

  def __init__(self, keys, prefix, filters, page, client):
    super(CARFChunk, self).__init__(keys, prefix)
    self.filters = filters
    self.page = page
    self.client = client

  def rows(self):
    result = self.client.fetch(self.filters, self.page)
    for record in result['response']['docs']:
      to_download=[]
      session_at = pendulum.parse(record['dt_sessao_tdt'])
      record_id = record['id']
      base_path = f'{session_at.year}/{session_at.month:02d}'
      report_id, _ = record['nome_arquivo_pdf_s'].split('.')
      dest_record = f"{base_path}/doc_{record_id}_{report_id}.json"
      
      report_url = f'https://acordaos.economia.gov.br/acordaos2/pdfs/processados/{report_id}.pdf'
      dest_report = f"{base_path}/doc_{record_id}_{report_id}.pdf"
      
      try:
        response = requests.get(report_url, verify=False)
        response.raise_for_status()
        if len(response.text) > MIN_FILE_SIZE:
          to_download.append(base.Content(content=response.content, content_type='application/pdf', dest=dest_report))
      except requests.exceptions.HTTPError:
        logger.warn(f'PDF not available for {record["numero_processo_s"]} - trying other source...')
        pdf_content=self.download_pdf_from_other_source(record)
        if len(pdf_content) > MIN_FILE_SIZE:
          to_download.append(base.Content(content=pdf_content, content_type='application/pdf', dest=dest_report))
      to_download.append(base.Content(content=json.dumps(record), dest=dest_record,
                       content_type='application/json'))
      yield to_download
    
  def download_pdf_from_other_source(self, record):
    
    def get_link_id(browser):
      browser.driver.implicitly_wait(20)
      process_links = browser.bsoup().find_all('a', text=record['numero_decisao_s'])
      if not len(process_links)==1:
        raise Exception(f"Couldn't get correct results page, number of links found: {len(process_links)}")
      return process_links[0]['id']

    def get_entry_page(browser, record):
      browser.get('https://carf.fazenda.gov.br/sincon/public/pages/ConsultarJurisprudencia/consultarJurisprudenciaCarf.jsf')
      browser.fill_in('valor_pesquisa1',record['numero_processo_s'])
      browser.driver.find_element(value='botaoPesquisarCarf').click()
      browser.driver.implicitly_wait(20)

    def get_act_page(browser):
      browser.driver.implicitly_wait(20)
      WebDriverWait(browser.driver, 10).until(EC.element_to_be_clickable((By.ID, 'tblJurisprudencia:0:j_id54_body')))
      link_id=get_link_id(browser)
      WebDriverWait(browser.driver, 10).until(EC.element_to_be_clickable((By.ID, link_id))).click()
      browser.driver.implicitly_wait(20)

    @utils.retryable()
    def get_pdf_content(browser):
      browser.driver.implicitly_wait(20)
      assert browser.bsoup().find(text='Anexos')
      data = {
          'formAcordaos': 'formAcordaos',
          'uniqueToken': '',
          'javax.faces.ViewState': 'j_id3',
          'formAcordaos:_idcl': 'formAcordaos:j_id60:0:j_id61',
      }
      try:
        response = requests.post(
            'https://carf.fazenda.gov.br/sincon/public/pages/ConsultarJurisprudencia/listaJurisprudencia.jsf',
            cookies=browser.get_cookie_dict(),
            headers=DEFAULT_PDF_HEADERS,
            data=data,
            verify=False,
        )
        response.raise_for_status()
        return response.content
      except requests.exceptions.HTTPError:
        raise utils.PleaseRetryException('Could not download PDF - retrying...')

    def get_process_pdf(record):
      with browsers.FirefoxBrowser(headless=True) as browser:
        browser.driver.implicitly_wait(20)
        get_entry_page(browser, record)
        browser.driver.implicitly_wait(20)
        get_act_page(browser)
        browser.driver.implicitly_wait(20)
        pdf_content = get_pdf_content(browser)
      return pdf_content

    return get_process_pdf(record)
    
   


    
    print(5)
      # try:
      #   # response = session.post(
      #   #     'https://carf.fazenda.gov.br/sincon/public/pages/ConsultarJurisprudencia/consultarJurisprudenciaCarf.jsf',
      #   #     data=urllib.parse.urlencode(data),
      #   #     verify=False,
      #   # )
      #   response.raise_for_status()
      #   soup = utils.soup_by_content(response.text)
        # print(5)
      # except requests.exceptions.HTTPError:
      #   raise utils.PleaseRetryException(f'Got {response.status_code} for form post - trying again')
      


@celery.task(name='crawlers.carf', default_retry_delay=5 * 60,
             autoretry_for=(BaseException,))
def carf_task(**kwargs):
  setup_cloud_logger(logger)

  from app.crawlers.logutils import logging_context

  with logging_context(crawler='carf'):
    output = utils.get_output_strategy_by_path(path=kwargs.get('output_uri'))
    logger.info(f'Output: {output}.')

    start_date = kwargs.get('start_date', '') + ' 00:00:00'
    end_date = kwargs.get('end_date', '') + ' 23:59:59'

    date_format = 'YYYY-MM-DD HH:mm:ss'

    start_date = pendulum.from_format(start_date, date_format).to_iso8601_string()
    end_date = pendulum.from_format(end_date, date_format).to_iso8601_string()
    time_interval = f'dt_sessao_tdt:[{start_date} TO {end_date}]'

    query_params = {
        'sort': 'id asc',
        'rows': 10,
        'wt': 'json',
        'fq': time_interval,
    }

    collector = CARFCollector(client=CARFClient(), filters=query_params)
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


@cli.command(name='carf')
@click.option('--start-date',
              default=utils.DefaultDates.BEGINNING_OF_YEAR_OR_SIX_MONTHS_BACK.strftime("%Y-%m-%d"),
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
def carf_command(**kwargs):
  enqueue, split_tasks = kwargs.get('enqueue'), kwargs.get('split_tasks')
  del (kwargs['enqueue'])
  del (kwargs['split_tasks'])
  if enqueue:
    utils.enqueue_tasks(carf_task, split_tasks, **kwargs)
  else:
    carf_task(**kwargs)
