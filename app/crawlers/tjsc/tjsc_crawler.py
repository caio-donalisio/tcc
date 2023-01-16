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



# response = requests.post(
#     'https://busca.tjsc.jus.br/jurisprudencia/buscaajax.do?&categoria=acordaos&categoria=acma&categoria=recurso',
#     cookies=cookies,
#     headers=headers,
#     data=data,
# )

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
#     {
#     'q': '',
#     'only_ementa': '',
#     'qualquer': '',
#     'excluir': '',
#     'prox1': '',
#     'prox2': '',
#     'proxc': '',
#     'frase': '',
#     'classe': '',
#     'juizProlator': '',
#     'origem': '',
#     'relator': '',
#     'radio_campo': 'ementa',
#     'faceta': 'true',
#     'busca': 'avancada',
#     'datainicial': start_date.format(SEARCH_DATE_FORMAT),
#     'datafinal': end_date.format(SEARCH_DATE_FORMAT),
#     'nuProcesso': '',
#     'ps': '50',
#     'sort': 'dtJulgamento desc',
# }


class NoProcessNumberError(Exception):
    pass

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
                }
            )

        except Exception as e:
            logger.error(f"page fetch error params: {filters}")
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
        for start,end in reversed(periods):
            total = self.count({'start_date':start,'end_date':end})
            pages = range(1, 2 + total//RESULTS_PER_PAGE)
            for page in pages:
                yield TJSCChunk(
                    keys={
                        'start_date':start.to_date_string(),
                        'end_date':end.to_date_string(),
                        'page': page,
                        'count': total,
                        },
                    prefix='',
                    filters={
                        'start_date':start,
                        'end_date':end,
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
        result = self.client.fetch(self.filters, self.page)
        soup = utils.soup_by_content(result.text)
        time.sleep(2)

        @utils.retryable(sleeptime=31)
        def check_if_recaptcha(soup):
            if soup.find(text=re.compile(r'.*O Poder Judiciário de Santa Catarina identificou inúmeros acessos provenientes do IP:.*')):
                raise utils.PleaseRetryException()
            
        check_if_recaptcha(soup)
        for act in soup.find_all('div', class_='resultados'):
            
            if soup.find('img', src=re.compile(r".*imagens/html.png")):
                html_link = act.find('a', href=re.compile(r".*html\.do.*"))
                html_link = BASE_URL + html_link.get('href') if html_link else ''

            if soup.find('img', src=re.compile(r".*imagens/pdf.png")):
                pdf_link = act.find('a', href=re.compile(r".*integra\.do.*arq=pdf"))
                pdf_link = BASE_URL + pdf_link.get('href') if pdf_link else ''
            
            if soup.find('img', src=re.compile(r".*imagens/rtf.png")):
                rtf_link = act.find('a', href=re.compile(r".*integra\.do.*?(?!arq=pdf)"))
                rtf_link = BASE_URL + rtf_link.get('href') if rtf_link else ''

            # process_code = utils.extract_digits(act.find(text=re.compile('.*Processo\:.*')).next.next.next.text)
            process_code = act.find(text=re.compile('.*Processo\:.*'))
            while not re.search(r'[\d\-\.]+', str(process_code)):
                process_code = process_code.next
            process_code = utils.extract_digits(str(process_code))
            

            assert process_code and len(process_code) > 5
            session_date = act.find(text=re.compile('.*Julgado em\:.*')).next
            session_date = pendulum.from_format(session_date.strip(), 'DD/MM/YYYY')
            
            act_id = act.find(onclick=re.compile(r".*abreIntegra.*\(.*\,\'.+\W.*")).get('onclick')
            act_id = re.search(r".*abreIntegra.*\(.*\,\'([\w\d]+)\W.*", act_id).group(1)

            base_path = f'{session_date.year}/{session_date.month}/{session_date.format("DD")}_{process_code}_{act_id}'
            
            if html_link:
                to_download.append(
                    base.ContentFromURL(src=html_link, dest=f'{base_path}_FULL.html', content_type='text/html')
                )

            if pdf_link:
                to_download.append(
                    base.ContentFromURL(src=pdf_link, dest=f'{base_path}.pdf', content_type='application/pdf')
                )
            
            if rtf_link:
                to_download.append(
                    base.ContentFromURL(src=rtf_link, dest=f'{base_path}.rtf', content_type='application/rtf')
                )
            
            #TJSC will cut connection if too many requests are made
            time.sleep(0.5)

            to_download.append(base.Content(
                content=str(act),
                dest=f'{base_path}.html',
                content_type='text/html'))


            yield to_download

@celery.task(name='crawlers.tjsc', default_retry_delay=5 * 60,
            autoretry_for=(BaseException,))
def tjsc_task(**kwargs):
    setup_cloud_logger(logger)

    from app.crawlers.logutils import logging_context

    with logging_context(crawler='tjsc'):
        output = utils.get_output_strategy_by_path(path=kwargs.get('output_uri'))
        logger.info(f'Output: {output}.')

        start_date = kwargs.get('start_date')
        start_date = pendulum.from_format(start_date,INPUT_DATE_FORMAT)#.format(SEARCH_DATE_FORMAT)
        end_date = kwargs.get('end_date')
        end_date = pendulum.from_format(end_date,INPUT_DATE_FORMAT)#.format(SEARCH_DATE_FORMAT)

        query_params = {
            'start_date':start_date,
            'end_date': end_date,
        }

        collector = TJSCCollector(client=TJSCClient(), filters=query_params)
        handler   = base.ContentHandler(output=output)
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
@click.option('--end-date'  ,
  default=utils.DefaultDates.NOW.strftime("%Y-%m-%d"),
  help='Format YYYY-MM-DD.',
)
@click.option('--output-uri',    default=None,     help='Output URI (e.g. gs://bucket_name')
@click.option('--enqueue'   ,    default=False,    help='Enqueue for a worker'  , is_flag=True)
@click.option('--split-tasks',
    default=None, help='Split tasks based on time range (weeks, months, days, etc) (use with --enqueue)')
def tjsc_command(**kwargs):
  # VALIDATE URI
  if COURT_NAME.lower() not in kwargs.get('output_uri').lower():
      if not click.confirm(
          (f"Name of court {COURT_NAME.upper()} not found in the "
            f"URI {kwargs.get('output_uri')}. REALLY PROCEED?"),default=False):
          logger.info('Operation canceled - wrong URI')
          return
  if kwargs.get('enqueue'):
    if kwargs.get('split_tasks'):
      start_date = pendulum.parse(kwargs.get('start_date'))
      end_date = pendulum.parse(kwargs.get('end_date'))
      for start, end in utils.timely(start_date, end_date, unit=kwargs.get('split_tasks'), step=1):
        task_id = tjsc_task.delay(
          start_date=start.to_date_string(),
          end_date=end.to_date_string(),
          output_uri=kwargs.get('output_uri'))
        print(f"task {task_id} sent with params {start.to_date_string()} {end.to_date_string()}")
    else:
      tjsc_task.delay(**kwargs)
  else:
    tjsc_task(**kwargs)
