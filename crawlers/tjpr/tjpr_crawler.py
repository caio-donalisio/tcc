import base
import re
import pendulum
import celery
import utils
from logconfig import logger_factory, setup_cloud_logger
import click
from app import cli, celery
import requests
from urllib.parse import parse_qsl, urlsplit


logger = logger_factory('tjpr')

COURT_NAME = 'tjpr'
RESULTS_PER_PAGE = 10  # DEFINED BY THEIR SERVER, SHOULDN'T BE CHANGED
INPUT_DATE_FORMAT = 'YYYY-MM-DD'
SEARCH_DATE_FORMAT = 'DD/MM/YYYY'
NOW = pendulum.now()
BASE_URL = 'https://portal.tjpr.jus.br'

DEFAULT_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:99.0) Gecko/20100101 Firefox/99.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    # 'Accept-Encoding': 'gzip, deflate, br',
    'Origin': 'https://portal.tjpr.jus.br',
    'Connection': 'keep-alive',
    'Referer': 'https://portal.tjpr.jus.br/jurisprudencia/',
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

def get_filters(start_date, end_date, **kwargs):
    return {
    'backURL': '',
    'postCampo': '',
    'tmp': '',
    'criterioPesquisa': '',
    'processo': '',
    'acordao': '',
    'idRelator': '',
    'nomeRelator': '',
    'idOrgaoJulgadorSelecao': '',
    'nomeOrgaoJulgador': '',
    'idComarca': '',
    'nomeComarca': '',
    'idClasseProcessual': '',
    'descricaoClasseProcessualHidden': '',
    'descricaoClasseProcessual': '',
    'idAssunto': '',
    'descricaoAssuntoHidden': '',
    'descricaoAssunto': '',
    'dataJulgamentoInicio': '',
    'dataJulgamentoFim': '',
    'dataPublicacaoInicio': start_date.format(SEARCH_DATE_FORMAT),
    'dataPublicacaoFim':  end_date.format(SEARCH_DATE_FORMAT),
    'idLocalPesquisa': '1',
    'ambito': '6',
    'idsTipoDecisaoSelecionados': '1',
    'segredoJustica': 'pesquisar sem',
    'mostrarCompleto': 'true',
    'iniciar': 'Pesquisar',
}

class TJPRClient:

    def __init__(self):
        self.session = requests.Session()
        self.url = f'{BASE_URL}/jurisprudencia/publico/pesquisa.do'

    @utils.retryable(max_retries=3)
    def count(self,filters):
        result = self.fetch(filters)
        soup = utils.soup_by_content(result.text)
        count_tag = soup.find('div', attrs={'class':'navLeft'})
        if count_tag:
            count = re.search(r'^\s*(\d+) registro.*$',count_tag.text)
            count = int(count.group(1))
        else:
            count = 0
        return count
    
    def count_periods(self,filters,unit='months'):
        return sum(1 for _ in utils.timely(
                    filters.get('start_date'),
                    filters.get('end_date'),
                    unit=unit,
                    step=1)
                )

    @utils.retryable(max_retries=3)
    def fetch(self, filters, page=1):
        import proxy
        self.session.headers.update(DEFAULT_HEADERS)
        try:
            return self.session.post(
                url=f'{self.url}',
                data={
                    **get_filters(**filters),
                    'pageNumber':str(page)
                },
                params = {
                'actionType': 'pesquisar',
                },
                proxies={'http':proxy.get_random_proxy()}
            )

        except Exception as e:
            logger.error(f"page fetch error params: {filters}")
            raise e


class TJPRCollector(base.ICollector):

    def __init__(self, client, filters):
        self.client = client
        self.filters = filters

    def count(self, period=None):
        if self.filters.get('count_only'):
            return self.client.count_periods(self.filters)
        elif period:
            return self.client.count(period)
        else:
            return self.client.count(self.filters)

    def chunks(self):
        from math import ceil
        periods = list(utils.timely(
            start_date=self.filters.get('start_date'),
            end_date=self.filters.get('end_date'),
            step=1,
            unit='months',
        ))
        for start,end in reversed(periods):
            total = self.count({'start_date':start,'end_date':end})
            pages = [1] if self.filters['count_only'] else range(1, 2 + total//RESULTS_PER_PAGE)
            for page in pages:
                yield TJPRChunk(
                    keys={
                        'start_date':start.to_date_string(),
                        'end_date':end.to_date_string(),
                        'page': page,
                        'count': total,
                        'count_only':self.filters.get('count_only'),
                        },
                    prefix='',
                    filters={
                        'start_date':start,
                        'end_date':end,
                        },
                    page=page,
                    count_only = self.filters.get('count_only'),
                    client=self.client
                )
class TJPRChunk(base.Chunk):

    def __init__(self, keys, prefix, filters, page, count_only, client):
        super(TJPRChunk, self).__init__(keys, prefix)
        self.filters  = filters
        self.page = page
        self.client = client
        self.count_only = count_only
    
    @utils.retryable()
    def rows(self):
        if self.count_only:
            count_data,count_filepath = utils.get_count_data_and_filepath(
                start_date=self.filters.get('start_date'),
                end_date=self.filters.get('end_date'),
                court_name=COURT_NAME,
                count = self.client.count(self.filters),
                count_time=NOW
                )
            yield utils.count_data_content(count_data,count_filepath)

        else:

            def get_act_id(soup):
                act_id = soup.find('b',text=re.compile(r'.*Processo.*')).next.next
                act_id = re.search(r'\s*([\.\d\-]+)\s*', act_id, re.DOTALL).group(1).replace('-','').replace('.','')
                return act_id
                
            def get_publication_date(soup):
                publication_date=  soup.find('b', text=re.compile(r'.*Data da Publicação.*')).next.next
                publication_date= re.search(r'.*((?P<day>\d{2})\/(?P<month>\d{2})\/(?P<year>\d{4})).*$', publication_date, re.DOTALL).groupdict()
                return publication_date

            result = self.client.fetch(self.filters,self.page)
            soup = utils.soup_by_content(result.text)
            acts = soup.find_all('table', class_='resultTable linksacizentados juris-dados-completos')
            to_download = []
            if not acts:
                raise utils.PleaseRetryException()
            for act in acts:
                act_id = get_act_id(act)
                publication_date = get_publication_date(act)
                ementa_hash = utils.get_content_hash(act, [{'name':'div','id':re.compile(r'ementa.*')}])
                # base_path = f'{publication_date["year"]}/{publication_date["month"]}/{publication_date["day"]}_{act_id}_{content_hash}'

                pdf_link = [link for link in act.find_all('a') if link.next.next=='Carregar documento']
                pdf_bytes, pdf_hash = self.download_pdf(pdf_link, act_id)
                base_path = f'{publication_date["year"]}/{publication_date["month"]}/{publication_date["day"]}_{act_id}_{ementa_hash}_{pdf_hash}'

                if pdf_bytes:
                    to_download.append(base.Content(
                        content=pdf_bytes,
                        dest=f'{base_path}.pdf',
                        content_type='application/pdf'))
                else:
                    logger.warn(f'Inteiro not available for {act_id}')

                to_download.append(base.Content(
                    content=str(act),
                    dest=f'{base_path}.html',
                    content_type='text/html'))

                yield to_download

    @utils.retryable()
    def download_pdf(self, pdf_link, act_id, hash_len=10):
                from io import BytesIO
                import zipfile
                from PyPDF2 import PdfFileMerger
                import hashlib

                if pdf_link:
                    pdf_link = pdf_link.pop()    
                    PATTERN = re.compile(r".*replace\(\'(.*?)\'\)")
                    relative_pdf_url = PATTERN.search(pdf_link['href']).group(1)
                    url = f"{BASE_URL}{relative_pdf_url}"
                    resp = requests.get(url)
                    try:
                        zipfile = zipfile.ZipFile(BytesIO(resp.content))
                    except zipfile.BadZipFile:
                        logger.warn(f'Could not download ZIP from {act_id}, retrying...')
                        raise utils.PleaseRetryException()
                        
                    merger = PdfFileMerger()
                    for file in sorted(zipfile.namelist()):
                        merger.append(zipfile.open(file))
                    pdf_bytes = BytesIO()
                    merger.write(pdf_bytes)
                    bytes_value = pdf_bytes.getvalue()
                    pdf_hash = hashlib.sha1(bytes_value).hexdigest()[:hash_len]
                else:
                    bytes_value = b''
                    pdf_hash = 0 * hash_len
                return bytes_value, pdf_hash


@celery.task(queue='crawlers.tjpr', default_retry_delay=5 * 60,
            autoretry_for=(BaseException,))
def tjpr_task(**kwargs):
    import utils
    setup_cloud_logger(logger)

    from logutils import logging_context

    with logging_context(crawler='tjpr'):
        output = utils.get_output_strategy_by_path(path=kwargs.get('output_uri'))
        logger.info(f'Output: {output}.')

        start_date = kwargs.get('start_date')
        start_date = pendulum.from_format(start_date,INPUT_DATE_FORMAT)#.format(SEARCH_DATE_FORMAT)
        end_date = kwargs.get('end_date')
        end_date = pendulum.from_format(end_date,INPUT_DATE_FORMAT)#.format(SEARCH_DATE_FORMAT)

        query_params = {
            'start_date':start_date,
            'end_date': end_date,
            'count_only':kwargs.get('count_only')
        }

        collector = TJPRCollector(client=TJPRClient(), filters=query_params)
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
@click.option('--start-date',    prompt=True,      help='Format YYYY-MM-DD.')
@click.option('--end-date'  ,    prompt=True,      help='Format YYYY-MM-DD.')
@click.option('--output-uri',    default=None,     help='Output URI (e.g. gs://bucket_name')
@click.option('--enqueue'   ,    default=False,    help='Enqueue for a worker'  , is_flag=True)
@click.option('--split-tasks',
    default=None, help='Split tasks based on time range (weeks, months, days, etc) (use with --enqueue)')
@click.option('--count-only', 
    default=False, help='Crawler will only collect the expected number of results', is_flag=True)
def tjpr_command(**kwargs):
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
        task_id = tjpr_task.delay(
          start_date=start.to_date_string(),
          end_date=end.to_date_string(),
          output_uri=kwargs.get('output_uri'))
        print(f"task {task_id} sent with params {start.to_date_string()} {end.to_date_string()}")
    else:
      tjpr_task.delay(**kwargs)
  else:
    tjpr_task(**kwargs)
