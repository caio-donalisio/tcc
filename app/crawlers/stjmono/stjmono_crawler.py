import re
import pendulum
import click
import requests
from typing import List
import bs4
from app.crawlers import base, utils, logutils

from app.crawler_cli import cli
from app.celery_run import celery_app as celery
from app.crawlers.logconfig import logger_factory, setup_cloud_logger
logger = logger_factory('stjmono')


NOW = pendulum.now().to_datetime_string()
DATE_FORMAT_1 = 'YYYYMMDD'
DATE_FORMAT_2 = 'DD/MM/YYYY'
DEFAULT_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6,ja;q=0.5',
    'Cache-Control': 'max-age=0',
    'Connection': 'keep-alive',
    # 'Cookie': 'f5_cspm=1234; JSESSIONID=W5hlk1-uDQhzLhR66JN3lNdNV5qDgolZorpGTyty.svlp-jboss-01; TS01603393=016a5b3833b3834c7fed9a309c8228bc1a920557481e443674e7e9316852bbd0daad16b56b1994c1eb74f44023598e2fe5cccd88d2; TS0133e456=0132b05847992e906502f2b8833d7a169f0ebe088934c98bf0763a9f29f7d5f055b1392f930b86f7493b394d6d72339819638f17d6a73b2f8f876a142ff729715452a228556f8da23a47c05c892f35379533d91462; _ga=GA1.3.84998084.1664279080; BIGipServerpool_svlp-jboss_scon=1057204416.36895.0000; BIGipServerpool_wserv=1023413514.20480.0000; _gid=GA1.3.1107844980.1673011176; TS01dc523b=016a5b38336cefb0eb88e7ceabe09af8ec89e1be4026faca030645367db23f7858fa343b8ed3d7a6094e1bb1e6566d976957c0e1e0; TS0122e3df=0132b058476d15365f25ba6d43f5b0af647c796990e69ae6548c91554175157ca85409b9576793e9bbcd3bcfbf4e472eb176fcf58ebc5dc9a4459ce136a3d98ccc16891973; TS0165095f=0132b0584735807162bbe8ab6f7337f5364d90a466f643011574ee91c4c5f4c7f32853929651abf57112855968226788ddc91dc689d976b98f8c8d2924baeb7c6eb0f4258624a25404def5a16d5a8ec9701c0c2cad41d0cccc47f112cc222773368f96dbbb; TS01e2b0fb=016a5b3833850a7a591750d7d5c3df75877543384ac77b4a16aafda6071dbe6533f9059f308778de66bc3b08572f87b5f6d09877c3; TS013f88e9=0132b058470d915587f5f9f285593b226b05e62b3634c98bf0763a9f29f7d5f055b1392f93e1a51c5a394448494de52da6ca4628da3fd96db8f2c611bba389bbdcdd9a0974',
    'Origin': 'https://scon.stj.jus.br',
    'Referer': 'https://scon.stj.jus.br/SCON/',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Mobile Safari/537.36 Edg/108.0.1462.54',
    'sec-ch-ua': '"Not?A_Brand";v="8", "Chromium";v="108", "Microsoft Edge";v="108"',
    'sec-ch-ua-mobile': '?1',
    'sec-ch-ua-platform': '"Windows"',
}

def get_filters(start_date : pendulum.DateTime, end_date : pendulum.DateTime):
  date_filter = f'@DTPB >= {start_date.format(DATE_FORMAT_1)} E @DTPB <= {end_date.format(DATE_FORMAT_1)}'
  return {
# 'pesquisaAmigavel':'+%3Cb%3EPublica%E7%E3o%3A+01%2F02%2F2022+a+01%2F03%2F2022%3C%2Fb%3E',
'acao': 'pesquisar',
'novaConsulta': 'true',
# 'i': 1,
'b': 'DTXT',
'livre': '',
'filtroPorOrgao': '',
'filtroPorMinistro': '',
'filtroPorNota': '',
'data': date_filter,
'operador': 'e',
'p': 'true',
'tp': 'T',
'processo': '',
'classe': '',
'uf': '',
'relator': '',
'dtpb': date_filter,
'dtpb1':start_date.format(DATE_FORMAT_2),
'dtpb2':end_date.format(DATE_FORMAT_2),
'dtde': '',
'dtde1': '',
'dtde2': '',
'orgao': '',
'ementa': '',
'nota': '',
'ref': '',
  }

def get_row_filters(start_date : pendulum.DateTime, end_date : pendulum.DateTime):
  date_filter = f'@DTPB >= {start_date.format(DATE_FORMAT_1)} E @DTPB <= {end_date.format(DATE_FORMAT_1)}'
  return {
    'numDocsPagina': '50',
    'tipo_visualizacao': '',
    'filtroPorNota': '',
    'ref': '',
    'p': 'true',
    'b': 'DTXT',
    'data': date_filter,
    # 'i': '11',
    # 'l': '10',
    'tp': 'T',
    'operador': 'E',
}

class STJMONOClient:

  def __init__(self):
    self.base_url  = 'https://scon.stj.jus.br'
    self.requester = requests.Session()
    self.requester.get("https://scon.stj.jus.br/SCON/", verify=False)

  def reset_session(self):
    self.requester = requests.Session()
    self.requester.get("https://scon.stj.jus.br/SCON/", verify=False)

  @utils.retryable(max_retries=3)
  def count(self, filters):
    response = self.fetch(filters, offset=0)
    return self._count_by_content(response.content)

  @utils.retryable(max_retries=3)
  def fetch(self, filters, offset):
    return self._response_or_retry(data={**filters, 'i': offset})

  @utils.retryable(max_retries=3)
  def get(self, path):
    return self.requester.get(f'{self.base_url}/{path}', verify=False)

  @utils.retryable(max_retries=3)
  def _response_or_retry(self, data):
    response = self.requester.post(
      f'{self.base_url}/SCON/pesquisar.jsp',
      headers=DEFAULT_HEADERS,
      verify=False,
      data=data)
    soup = utils.soup_by_content(response.content)

    if soup \
        .find('div', id='idCaptchaLinha'):
      logger.warn('Got captcha -- reseting session.')
      self.reset_session()
      raise utils.PleaseRetryException()

    info = soup.find('span', {'class': 'numDocs'}) or \
      soup.find('div', {'class':'erroMensagem'})
    if not info:
      logger.warn('Got invalid page -- reseting session.')
      self.reset_session()
      raise utils.PleaseRetryException()

    return response

  @utils.retryable(max_retries=3)
  def _response_or_retry_rows(self, data):
    self.requester.get("https://scon.stj.jus.br/SCON/", verify=False, headers=DEFAULT_HEADERS)
    response = self.requester.post(
      f'{self.base_url}/SCON/decisoes/toc.jsp',
      headers=DEFAULT_HEADERS,
      verify=False,
      data=data)
    soup = utils.soup_by_content(response.content)

    if soup \
        .find('div', id='idCaptchaLinha'):
      logger.warn('Got captcha -- reseting session.')
      self.reset_session()
      raise utils.PleaseRetryException()

    info = soup.find('span', {'class': 'numDocs'}) or \
      soup.find('div', {'class':'erroMensagem'})
    if not info:
      logger.warn('Got invalid page -- reseting session.')
      self.reset_session()
      raise utils.PleaseRetryException()

    return response


  def fetch_rows(self, filters, offset):
    return self._response_or_retry_rows(data={**filters, 'i': offset})

  def _count_by_content(self, content):
    soup = utils.soup_by_content(content)
    info = soup.find('span', text=re.compile(r'.*monocrátic.*'))
    assert not soup.find('div', {'class':'erroMensagem'})
    assert info
    return int(utils.extract_digits(info.text))

class STJMONOCollector(base.ICollector):

  def __init__(self, client : STJMONOClient, query : dict, **options):
    self.client  = client
    self.query   = query
    self.options = (options or {})

  def count(self) -> int:
    return self.client.count(get_filters(
      self.query['start_date'], self.query['end_date']))

  def chunks(self):
    ranges = list(utils.timely(
      self.query['start_date'], self.query['end_date'], unit='days', step=1))

    for start_date, end_date in reversed(ranges):
      filters = get_row_filters(start_date, end_date)
      count   = self.client.count(filters)

      docs_per_page = 50
      for offset in range(0, count + 1, docs_per_page):
        offset = docs_per_page if not offset else offset + 1
        keys =\
          {'start_date' : start_date.to_date_string(),
            'end_date'  : end_date.to_date_string(),
            'offset'    : offset,
            'limit'     : count + 1}

        yield STJMONOChunk(keys=keys,
          client=self.client,
          filters=filters,
          docs_per_page=docs_per_page,
          limit=count + 1,
          prefix=f'{start_date.year}/{start_date.month:02d}/')


class STJMONOChunk(base.Chunk):

  def __init__(self, keys, client, filters, docs_per_page, limit, prefix):
    super(STJMONOChunk, self).__init__(keys, prefix)
    self.client  = client
    self.filters = filters
    self.docs_per_page   = docs_per_page
    self.limit   = limit

  @utils.retryable(max_retries=3)
  def rows(self):

    response = self.client.fetch_rows({
      **self.filters, **{'l': self.docs_per_page, 'numDocsPagina': self.docs_per_page}}, 
      offset=self.keys['offset'])
    
    soup  = utils.soup_by_content(response.content)
    count = self.client._count_by_content(response.content)
    if count == 0:
      return []
    for content in self.page_contents(soup):
      yield content

  def page_contents(self, soup):
    

    @utils.retryable()
    def _get_pdf_urls(doc):
      BASE_PDF_URL = "https://processo.stj.jus.br"
      a = doc.find('a',attrs={'title':'Decisão Monocrática Certificada'}) or \
        doc.find('a',attrs={'original-title':'Decisão Monocrática Certificada'})
      pdfs_page = utils.get_response(logger, requests.Session(), utils.find_between(a['href'], start="'", end="'"),'', verify=False)
      a_s = utils.soup_by_content(pdfs_page.text).find_all('a', text='Decisão Monocrática')
      pdf_links = [BASE_PDF_URL + utils.find_between(a['href'], start="'", end="'") for a in a_s]
      return pdf_links

    @utils.retryable()
    def get_direct_links(pdf_links: List) -> List:
      direct_links = []
      for pdf_link in pdf_links:
        page = utils.get_response(logger, requests.Session(), pdf_link, '', verify=False)
        soup = utils.soup_by_content(page.content)
        direct_links.append(soup.find('iframe')['src'])
      return direct_links

    def append_metadata(doc, doc_count):
      new_tag=bs4.Tag(name='div', attrs={'class':'expected-count'})
      new_tag.append(f'Number of expected documents: {doc_count}')
      doc.insert(1, new_tag)
      return doc

    docs = soup.find_all(class_='documento')

    for doc in docs:
      pdf_urls = _get_pdf_urls(doc)
      expected_doc_count = len(pdf_urls)

      to_download = []
      
      if expected_doc_count == 0:
        logger.warn(f'No document found. {str(doc)[:400]}')
        continue     
      
      #Checks if all num_registros are equal - should be.
      try:
        assert all(
          utils.get_param_from_url(pdf_urls[0], 'num_registro') == utils.get_param_from_url(url, 'num_registro') 
          for url in pdf_urls)
      except AssertionError:
        logger.error(f'Found links with different num_registro. Skipped. {pdf_urls=}')
        continue

      #Número processo
      act_id  = utils.get_param_from_url(pdf_urls[0], 'num_registro')
      
      #Seq
      seq = utils.get_param_from_url(pdf_urls[0], 'sequencial')
      
      #Date
      publication_date = utils.get_param_from_url(pdf_urls[0], 'data')
      date_obj = pendulum.from_format(publication_date, 'YYYYMMDD')
      day, month, year = date_obj.format('DD'), date_obj.format('MM'), date_obj.format('YYYY')

      #Componente
      componente = utils.get_param_from_url(pdf_urls[0], 'componente')
      if componente != "MON":
        logger.warn(f'Componente is not MON: {act_id=} {componente=} {seq=}')
      
      #Meta Hash
      meta_hash = utils.get_content_hash(doc, [{'name':'p'}])
      
      #Make filename
      filename = f'{year}/{month}/{day}_{componente}_{act_id}_{meta_hash}'

      #Get PDF data
      if expected_doc_count > 1:
        logger.warn(f'Found {expected_doc_count} documents, expected 1. {act_id=}')
        for n, pdf_url in enumerate(get_direct_links(pdf_urls), start=1):
          to_download.append(base.ContentFromURL(
            src=pdf_url, dest=f'{filename}_{n:02}.pdf', content_type='application/pdf')
            )
            
      elif expected_doc_count == 1:
        to_download.append(base.ContentFromURL(
          src=get_direct_links(pdf_urls).pop(), dest=f'{filename}.pdf', content_type='application/pdf')
          )

    #Get Metadata
      to_download.append(
            base.Content(
              content=append_metadata(doc, expected_doc_count).prettify(), 
              dest=f'{filename}.html', 
              content_type='text/html'))

      yield to_download

@celery.task(queue='crawlers.stjmono', default_retry_delay=5 * 60,
             autoretry_for=(BaseException,))
def stjmono_task(start_date, end_date, output_uri):
  setup_cloud_logger(logger)
 
  from app.crawlers.logutils import logging_context
  
  with logging_context(crawler='STJMONO'):
    output = utils.get_output_strategy_by_path(path=output_uri)
    logger.info(f'Output: {output}.')

    start_date, end_date =\
      pendulum.parse(start_date), pendulum.parse(end_date)

    query_params = {'start_date': start_date, 'end_date': end_date}
    collector = STJMONOCollector(client=STJMONOClient(), query=query_params)
    handler   = base.ContentHandler(output=output)

    snapshot = base.Snapshot(keys=query_params)
    base.get_default_runner(
        collector=collector, output=output, handler=handler, logger=logger, max_workers=8) \
      .run(snapshot=snapshot)


@cli.command(name='stjmono')
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
def stjmono_command(start_date, end_date, output_uri, enqueue, split_tasks):
  args = (start_date, end_date, output_uri)
  if enqueue:
    if split_tasks:
      start_date, end_date =\
        pendulum.parse(start_date), pendulum.parse(end_date)
      for start, end in reversed(list(utils.timely(start_date, end_date, unit=split_tasks, step=1))):
        task_id = stjmono_task.delay(
          start.to_date_string(),
          end.to_date_string(),
          output_uri)
        print(f"task {task_id} sent with params {start.to_date_string()} {end.to_date_string()}")
    else:
      stjmono_task.delay(*args)
  else:
    stjmono_task(*args)