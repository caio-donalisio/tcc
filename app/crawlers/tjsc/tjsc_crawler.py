from app.crawlers import base, utils, proxy
import re
import pendulum
from app.crawlers.logconfig import logger_factory, setup_cloud_logger
import click
from app.crawler_cli import cli
from app.celery_run import celery_app as celery
import requests
import time

logger = logger_factory('tjsc')

COURT_NAME = 'tjsc'
RESULTS_PER_PAGE = 50
INPUT_DATE_FORMAT = 'YYYY-MM-DD'
SEARCH_DATE_FORMAT = 'DD/MM/YYYY'
NOW = pendulum.now()
BASE_URL = 'https://busca.tjsc.jus.br/jurisprudencia/'
DOC_TO_PDF_CONTAINER_URL = 'http://localhost/unoconv/pdf'

DEFAULT_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:99.0) Gecko/20100101 Firefox/99.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    # 'Accept-Encoding': 'gzip, deflate, br',
    'Origin': 'https://portal.tjsc.jus.br',
    'Connection': 'keep-alive',
    'Referer': 'https://portal.tjsc.jus.br/jurisprudencia/',
    # Requests sorts cookies= alphabetically
    # 'Cookie': 'JSESSIONID=a2577f334c309880f8481b48ac29; _ga=GA1.3.1119819283.1654806370; _gid=GA1.3.346843250.1654806370',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-User': '?1',
    # Requests doesn't support trailers
    # 'TE': 'trailers',
}

DOWNLOAD_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6,ja;q=0.5',
    'Cache-Control': 'max-age=0',
    'Connection': 'keep-alive',
    # 'Cookie': 'JSESSIONID=72DC72D9050FC324EAA6B2B444228270; _ga_M10DPR56QV=GS1.1.1675107176.6.0.1675107752.0.0.0; _ga=GA1.3.1806411352.1673869367; _gid=GA1.3.2051174715.1675714634; _gat=1',
    'Referer': 'https://busca.tjsc.jus.br/jurisprudencia/',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Mobile Safari/537.36 Edg/109.0.1518.78',
    'sec-ch-ua': '"Not_A Brand";v="99", "Microsoft Edge";v="109", "Chromium";v="109"',
    'sec-ch-ua-mobile': '?1',
    'sec-ch-ua-platform': '"Android"',
}

def get_filters(start_date, end_date, **kwargs):
    return {
    'q': '',
    'only_ementa': '',
    'frase': '',
    'excluir': '',
    'qualquer': '',
    '': '',
    'prox1': '',
    'prox2': '',
    'proxc': '',
    'sort': 'dtJulgamento desc',
    'ps': '50',
    'busca': 'avancada',
    #'pg': '2',
    'flapto': '1',
    'datainicial': start_date.format(SEARCH_DATE_FORMAT),
    'datafinal': end_date.format(SEARCH_DATE_FORMAT),
    'radio_campo': 'integra',
    'categoria[]': [
        'acordaos',
        'acma',
        'recurso',
    ],
    'faceta': 'false',
}

class TJSCClient:

    def __init__(self):
        self.session = requests.Session()
        self.url = f'https://busca.tjsc.jus.br/jurisprudencia/buscaajax.do'

    @utils.retryable(max_retries=3, sleeptime=31)
    def count(self,filters, min_votes=3):
        counts = []
        while not any(counts.count(n) >= min_votes for n in counts):
            result = self.fetch(filters)
            soup = utils.soup_by_content(result.text)
            count_tag = soup.find('div', attrs={'class':'texto_resultados'})
            if count_tag:
                count = re.search(r'^\s*([\d\,]+) resultados.*$',count_tag.text, re.M)
                count = int(utils.extract_digits(count.group(1)))
            else:
                count = 0
            counts.append(count)
        def check_if_recaptcha(soup):
            if soup.find(text=re.compile(r'.*O Poder Judiciário de Santa Catarina identificou inúmeros acessos provenientes do IP:.*')):
                raise utils.PleaseRetryException()
        check_if_recaptcha(soup)
        return max(counts, key=lambda n: counts.count(n))

    @utils.retryable(max_retries=3)
    def fetch(self, filters, page=1):
        self.session.headers.update(DEFAULT_HEADERS)
        try:
            return self.session.post(
                url=f'{self.url}',
                data={
                    **get_filters(**filters),
                    'pg':str(page)
                },
                verify=False,
                timeout=10,
            )

        except Exception as e:
            logger.error(f"page fetch error params: {filters} {e}")
            raise e


class TJSCCollector(base.ICollector):

  def __init__(self, client, filters):
    self.client = client
    self.filters = filters

  def count(self, period=None):
    if period:
      return self.client.count(period)
    else:
      return self.client.count(self.filters)

  @utils.retryable()
  def chunks(self):
    periods = list(utils.timely(
        start_date=self.filters.get('start_date'),
        end_date=self.filters.get('end_date'),
        step=1,
        unit='months',
    ))
    time.sleep(3)
    for start, end in reversed(periods):
      total = self.count({'start_date': start, 'end_date': end})
      pages = range(1, 2 + total//RESULTS_PER_PAGE)
      for page in pages:
        yield TJSCChunk(
            keys={
                'start_date': start.to_date_string(),
                'end_date': end.to_date_string(),
                'page': page,
                'count': total,
            },
            prefix='',
            filters={
                'start_date': start,
                'end_date': end,
            },
            page=page,
            client=self.client
        )


class TJSCChunk(base.Chunk):

    def __init__(self, keys, prefix, filters, page, client):
        super(TJSCChunk, self).__init__(keys, prefix)
        self.filters  = filters
        self.page = page
        self.client = client

    @utils.retryable(sleeptime=31)
    def rows(self):
        to_download = []

        @utils.retryable(sleeptime=61, max_retries=5, message='Found ReCaptcha')
        def check_if_recaptcha():
            result = self.client.fetch(self.filters, self.page)
            soup = utils.soup_by_content(result.text)
            if soup.find(text=re.compile(r'.*O Poder Judiciário de Santa Catarina identificou inúmeros acessos provenientes do IP:.*')):
                raise utils.PleaseRetryException('Found ReCaptcha')
            return soup
        
        soup = check_if_recaptcha()
        
        for act in soup.find_all('div', class_='resultados'):
            time.sleep(0.2456)
            links = {}

            links['html'] = act.find('a', href=re.compile(r".*html\.do.*"))
            links['html'] = BASE_URL + links['html'].get('href') if links['html'] else ''

            links['pdf'] = act.find('a', href=re.compile(r".*integra\.do.*arq=pdf"))
            links['pdf'] = BASE_URL + links['pdf'].get('href') if links['pdf'] else ''
            
            links['rtf'] = act.find('a', href=re.compile(r"(.*integra\.do\?.*)[^(arq\=pdf)]+$"))
            links['rtf'] = BASE_URL + links['rtf'].get('href') if links['rtf'] else ''

            process_code = re.search(r'.*Processo\:\s?([\d\-\.\/]+)\s.*', act.find('p').text).group(1)
            process_code = utils.extract_digits(process_code)
            

            assert process_code and 25 > len(process_code) > 5
            session_date = act.find(text=re.compile('.*Julgado em\:.*')).next
            session_date = pendulum.from_format(session_date.strip(), 'DD/MM/YYYY')
            
            onclick = act.find(href='#', onclick=re.compile(r'abreIntegra'))['onclick']
            ajax, act_code, categoria, act_id = re.findall(r'\'([^\']+?)\'', onclick, re.U)
            links['short_html'] = f'https://busca.tjsc.jus.br/jurisprudencia/html.do?ajax={ajax}&id={act_code}&categoria={categoria}&busca=avancada'


            base_path = f'{session_date.year}/{session_date.month:02}/{session_date.format("DD")}_{process_code}_{act_id}'
            
            if links.get('short_html'):
                to_download.append(
                    base.ContentFromURL(src=links['short_html'], dest=f'{base_path}_short.html', content_type='text/html')
                )

            if links.get('html'):
                to_download.append(
                    base.ContentFromURL(src=links['html'], dest=f'{base_path}_FULL.html', content_type='text/html')
                )

            if links.get('pdf'):
                to_download.append(
                    base.ContentFromURL(src=links['pdf'], dest=f'{base_path}.pdf', content_type='application/pdf')
                )
            
            if links.get('rtf'):
                to_download.append(
                    base.ContentFromURL(src=links['rtf'], dest=f'{base_path}.rtf', content_type='application/rtf')
                )
            
            #TJSC will cut connection if too many requests are made
            time.sleep(0.261616136)

            to_download.append(base.Content(
                content=str(act),
                dest=f'{base_path}.html',
                content_type='text/html'))

            yield to_download

class TJSCContentHandler(base.ContentHandler):

  def __init__(self, output):
    self.output = output

  def validate_content(self, content:str):
    soup = utils.soup_by_content(content)
    captcha_pattern = r'.*O Poder Judiciário de Santa Catarina identificou inúmeros acessos provenientes do IP:.*'
    captcha_div = soup.find(text=re.compile(captcha_pattern))
    return not bool(captcha_div)

  @utils.retryable(max_retries=9, sleeptime=1.1)
  def _handle_url_event(self, event : base.ContentFromURL):
    if self.output.exists(event.dest):
      return

    try:
      response = requests.get(event.src,
        allow_redirects=False,
        headers = DOWNLOAD_HEADERS,
        verify=False,
        timeout=5.1,
)   
    except Exception as e:
      raise utils.PleaseRetryException(f'Could not download: {event.dest} {e}')
    if response.status_code == 404:
      return
    if not self.validate_content(response.text): 
      raise utils.PleaseRetryException('Invalid downloaded content')

    dest = event.dest
    content_type = event.content_type

    if response.status_code == 200 and len(response.content) > 0:
      self.output.save_from_contents(
          filepath=dest,
          contents=response.content,
          content_type=content_type)


@celery.task(name='crawlers.tjsc', default_retry_delay=5 * 60,
             autoretry_for=(BaseException,))
def tjsc_task(**kwargs):
  setup_cloud_logger(logger)

  from app.crawlers.logutils import logging_context

  with logging_context(crawler='tjsc'):
    output = utils.get_output_strategy_by_path(path=kwargs.get('output_uri'))
    logger.info(f'Output: {output}.')

    start_date = kwargs.get('start_date')
    start_date = pendulum.from_format(start_date,INPUT_DATE_FORMAT)
    end_date = kwargs.get('end_date')
    end_date = pendulum.from_format(end_date,INPUT_DATE_FORMAT)

    query_params = {
        'start_date': start_date,
        'end_date': end_date,
    }

    collector = TJSCCollector(client=TJSCClient(), filters=query_params)
    handler = TJSCContentHandler(output=output)
    snapshot = base.Snapshot(keys=query_params)

    base.get_default_runner(
        collector=collector,
        output=output,
        handler=handler,
        logger=logger,
        max_workers=8) \
        .run(snapshot=snapshot)


@cli.command(name=COURT_NAME)
@click.option('--start-date',
              default=utils.DefaultDates.THREE_MONTHS_BACK.strftime("%Y-%m-%d"),
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
def tjsc_command(**kwargs):
  enqueue, split_tasks = kwargs.get('enqueue'), kwargs.get('split_tasks')
  del (kwargs['enqueue'])
  del (kwargs['split_tasks'])
  if enqueue:
    utils.enqueue_tasks(tjsc_task, split_tasks, **kwargs)
  else:
    tjsc_task(**kwargs)
