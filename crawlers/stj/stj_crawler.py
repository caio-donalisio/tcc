import re
import base
import utils
import pendulum
import click
import requests
from app import cli, celery

from logconfig import logger_factory, setup_cloud_logger
logger = logger_factory('stj')


NOW = pendulum.now().to_datetime_string()
DATE_FORMAT_1 = 'YYYYMMDD'
DATE_FORMAT_2 = 'DD/MM/YYYY'
DEFAULT_HEADERS = {
    'Accept': '*/*',
    'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6,ja;q=0.5',
    'Connection': 'keep-alive',
    # Requests sorts cookies= alphabetically
    # 'Cookie': 'f5_cspm=1234; JSESSIONID=M68oQ4MRNooLU5mtmFT7sufU9q9p_x_NrpA4zKtM.svlp-jboss-01; TS01603393=016a5b3833b6deadb7303487d001db690ceda463c66cdfea251a48d9ef1e16efbd346eb5a247ded3c2d656fc81314481b822eaf2785251c22078f999ecc4f9fcd1521c483f; _ga=GA1.3.922156914.1655814167; _hjSessionUser_2631084=eyJpZCI6IjlkODdmNDJmLWZkZWUtNTgwYS1hNTU2LTQ3NDBkMjdiNTg4NCIsImNyZWF0ZWQiOjE2NTg0MjM0OTkyODMsImV4aXN0aW5nIjpmYWxzZX0=; BIGipServerpool_svlp-jboss_scon=1057204416.36895.0000; _gid=GA1.3.587150248.1660567534; TS0136de9a=016a5b3833b378f1b7adab4428037fcb6fabc787e401dcbb96ffee844656a5242a4df28d71578f195ed6a437f7897ef39fd4a3d01995c107ec83e61b66392295553039b2344a51ffe3496699965d1a60c80c8025ea; _gat_UA-179972319-1=1',
    'Referer': 'https://scon.stj.jus.br/SCON/pesquisar.jsp',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.5112.81 Safari/537.36 Edg/104.0.1293.54',
    'sec-ch-ua': '"Chromium";v="104", " Not A;Brand";v="99", "Microsoft Edge";v="104"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
}

def get_filters(start_date : pendulum.DateTime, end_date : pendulum.DateTime):
  date_filter = f'@DTPB >= {start_date.format(DATE_FORMAT_1)} E @DTPB <= {end_date.format(DATE_FORMAT_1)}'
  return {
# 'pesquisaAmigavel':'+%3Cb%3EPublica%E7%E3o%3A+01%2F02%2F2022+a+01%2F03%2F2022%3C%2Fb%3E',
'acao': 'pesquisar',
'novaConsulta': 'true',
# 'i': 1,
'b': 'ACOR',
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

class STJClient:

  def __init__(self):
    self.base_url  = 'https://scon.stj.jus.br'
    self.requester = requests.Session()

  def reset_session(self):
    self.requester = requests.Session()

  @utils.retryable(max_retries=3)
  def count(self, filters):
    response = self._response_or_retry(
      self.fetch(filters, offset=0))
    return self._count_by_content(response.content)

  @utils.retryable(max_retries=3)
  def fetch(self, filters, offset):
    return self._response_or_retry(self.requester.post(
      f'{self.base_url}/SCON/pesquisar.jsp',
      headers=DEFAULT_HEADERS,
      data={**filters, 'i': offset}))

  @utils.retryable(max_retries=3)
  def get(self, path):
    return self.requester.get(f'{self.base_url}/{path}')

  @utils.retryable(max_retries=3)
  def _response_or_retry(self, response):
    soup = utils.soup_by_content(response.content)

    if soup \
        .find('div', id='idCaptchaLinha'):
      logger.warn('Got captcha -- reseting session.')
      self.reset_session()
      raise utils.PleaseRetryException()

    return response

  def _count_by_content(self, content):
    soup = utils.soup_by_content(content)
    info = soup.find('span', {'class': 'numDocs'}) or \
      soup.find('div', {'class':'erroMensagem'})

    if not info:
      logger.warn(f"Missing info on: {content}")
      raise utils.PleaseRetryException()

    elif info.get_text() == 'Nenhum documento encontrado!':
      count = 0

    else:
      match = re.match(r'(\d+\.?\d+)', info.get_text())
      if match:
        count = int(match.group(0).replace('.', ''))
      else:
        count = 0

    return count


class STJCollector(base.ICollector):

  def __init__(self, client : STJClient, query : dict, **options):
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
      filters = get_filters(start_date, end_date)
      count   = self.client.count(filters)

      keys =\
        {'start_date' : start_date.to_date_string(),
          'end_date'  : end_date.to_date_string(),
          'limit'     : count + 1}

      yield STJChunk(keys=keys,
        client=self.client,
        filters=filters,
        limit=count + 1,
        prefix=f'{start_date.year}/{start_date.month:02d}/')


class STJChunk(base.Chunk):

  def __init__(self, keys, client, filters, limit, prefix):
    super(STJChunk, self).__init__(keys, prefix)
    self.client  = client
    self.filters = filters
    self.limit   = limit

  @utils.retryable(max_retries=3)
  def rows(self):
    response = self.client.fetch({
      **self.filters, **{'l': self.limit, 'numDocsPagina': self.limit}}, offset=0)
    soup  = utils.soup_by_content(response.content)
    count = self.client._count_by_content(response.content)
    if count == 0:
      return

    for content in self.page_contents(soup):
      yield content

  def page_contents(self, soup):
    docs = soup.find_all(class_='documento')

    def _get_pdf_url(doc):
      a = doc.find('a',attrs={'title':'Exibir o inteiro teor do acórdão.'}) or \
        doc.find('a',attrs={'original-title':'Exibir o inteiro teor do acórdão.'})
      return utils.find_between(a['href'], start="'", end="'")

    for doc in docs:
      pdf_url = _get_pdf_url(doc)
      act_id  = utils.get_param_from_url(pdf_url, 'num_registro')
      publication_date = utils.get_param_from_url(
        pdf_url, 'dt_publicacao')
      filepath = utils.get_filepath(publication_date, act_id, 'html')

      pdf_contents = self.pdf_contents(pdf_url, act_id, publication_date)
      yield [
        base.Content(
          content=doc.prettify(), dest=filepath, content_type='text/html'
        ),
        *pdf_contents
      ]

  def pdf_contents(self, pdf_path, act_id, publication_date):
    response = self.client.get(pdf_path)
    contents = []

    if 'text/html' in response.headers['Content-Type']:
      soup  = utils.soup_by_content(response.content)
      table = soup.find(id='listaInteiroTeor')
      if not table:
        logger.warn(f"{pdf_path} has no valid content.")
        return []

      rows  = table.find_all('div', class_='row')[1:]

      for index, row in enumerate(rows):
        url      = row.a['href'].replace('®', '&reg')
        if 'documento_sequencial' in url:
          doc_id   = utils.get_param_from_url(url, 'documento_sequencial')
        elif 'seq' in url:
          doc_id   = utils.get_param_from_url(url, 'seq')
        else:
          doc_id = 'NA'
        filename = f'{act_id}-{doc_id}--{index}'
        contents.append(base.ContentFromURL(
          src=url,
          dest=utils.get_filepath(
            date=publication_date, filename=filename, extension='pdf'
          ),
          content_type='application/pdf'
        ))

    else:
      pdf_filepath = utils.get_filepath(
        date=publication_date, filename=act_id, extension='pdf')

      contents.append(base.Content(
        content=response.content,
        content_type='application/pdf',
        dest=pdf_filepath
      ))

    return contents


@celery.task(queue='crawlers.stj', default_retry_delay=5 * 60,
             autoretry_for=(BaseException,))
def stj_task(start_date, end_date, output_uri):
  setup_cloud_logger(logger)

  from logutils import logging_context

  with logging_context(crawler='STJ'):
    output = utils.get_output_strategy_by_path(path=output_uri)
    logger.info(f'Output: {output}.')

    start_date, end_date =\
      pendulum.parse(start_date), pendulum.parse(end_date)

    query_params = {'start_date': start_date, 'end_date': end_date}
    collector = STJCollector(client=STJClient(), query=query_params)
    handler   = base.ContentHandler(output=output)

    snapshot = base.Snapshot(keys=query_params)
    base.get_default_runner(
        collector=collector, output=output, handler=handler, logger=logger, max_workers=8) \
      .run(snapshot=snapshot)


@cli.command(name='stj')
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
def stj_command(start_date, end_date, output_uri, enqueue, split_tasks):
  args = (start_date, end_date, output_uri)
  if enqueue:
    if split_tasks:
      start_date, end_date =\
        pendulum.parse(start_date), pendulum.parse(end_date)
      for start, end in reversed(list(utils.timely(start_date, end_date, unit=split_tasks, step=1))):
        task_id = stj_task.delay(
          start.to_date_string(),
          end.to_date_string(),
          output_uri)
        print(f"task {task_id} sent with params {start.to_date_string()} {end.to_date_string()}")
    else:
      stj_task.delay(*args)
  else:
    stj_task(*args)
