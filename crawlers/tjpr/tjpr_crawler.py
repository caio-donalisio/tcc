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

# BASE_URL = 'https://portal.tjpr.jus.br'
# LAWSUITS_URL = 'https://portal.tjpr.jus.br/jurisprudencia/publico/pesquisa.do?actionType=pesquisar'

# JUDGMENT_ID_SELECTOR = 'div.juris-tabela-propriedades > a'
# DECISION_ID_SELECTOR = 'input[name="id"]'
# DECISION_ABSTRACT_SELECTOR = '#ementa'
# DECISION_FULL_CONTENT_SELECTOR = '#texto'
# DECISION_TABLE_DATA_SELECTOR = 'table.resultTable.linksacizentados.juris-dados-completos > tbody > tr > td'

# DECISION_FIELDS = {
#   'Relator(a):': 'reportingJudge',
#   'Órgão Julgador:': 'judgingBody',
#   'Data do Julgamento:': 'decisionDate',
#   'Fonte/Data da Publicação:': 'publishingDate'
# }

# import requests
# from bs4 import BeautifulSoup

# data = {
#   "idLocalPesquisa": 99,
#   "ambito": 6,
#   "idsTipoDecisaoSelecionados": 1,
#   "segredoJustica": 'pesquisar com',
#   "iniciar": 'Pesquisar',
#   "pageSize": 10,
#   "pageNumber": 1,
# }

# response = requests.post(LAWSUITS_URL, data)
# soup = BeautifulSoup(response.text, 'html.parser')

# lawsuits = []

# for judgment_id in soup.select(JUDGMENT_ID_SELECTOR):
#   path = judgment_id.get('href')
#   url = f'{BASE_URL}{path}'
#   judgmentId = judgment_id.contents[0].strip()

#   lawsuits.append({ 'judgmentId': judgmentId, 'url': url })

# lawsuits

# decisions = []

# for lawsuit in lawsuits:
#   response = requests.get(lawsuit['url'])
#   soup = BeautifulSoup(response.text, 'html.parser')

#   decision = {}

#   decision['judgmentId'] = lawsuit['judgmentId']
#   decision['courtId'] = 'TJPR'
#   id = soup.select_one(DECISION_ID_SELECTOR)['value']
#   decision['abstract'] = soup.select_one(f'{DECISION_ABSTRACT_SELECTOR}{id}').text.strip()
#   decision['fullContent'] = soup.select_one(f'{DECISION_FULL_CONTENT_SELECTOR}{id}').text.strip()
  
#   for element in soup.select(DECISION_TABLE_DATA_SELECTOR):
#     for child in element.children:
#       if (child.name == 'b'):
#         field = child.text
#         value = [text for text in element.stripped_strings][1]
        
#         if (field in DECISION_FIELDS):
#           decision[DECISION_FIELDS[field]] = value

#   decisions.append(decision)

# decisions
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
    'ambito': '-1',
    'idsTipoDecisaoSelecionados': '1',
    'segredoJustica': 'pesquisar com',
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
                }
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
            result = self.client.fetch(self.filters,self.page)
            soup = utils.soup_by_content(result.text)
            act_links = soup.find_all('a',class_="acordao negrito")
            to_download = []
            for act_link in act_links:
                response = requests.get(f"{BASE_URL}{act_link['href']}", 
                    headers=DEFAULT_HEADERS)

                act_soup = utils.soup_by_content(response.text)
                act_id = act_soup.find('b',text=re.compile(r'.*Processo.*')).next.next
                act_id = re.search(r'\s*([\.\d\-]+)\s*', act_id, re.DOTALL).group(1).replace('-','').replace('.','')
                publication_date=  act_soup.find('b', text=re.compile(r'.*Data da Publicação.*')).next.next
                publication_date= re.search(r'.*((?P<day>\d{2})\/(?P<month>\d{2})\/(?P<year>\d{4})).*$', publication_date, re.DOTALL).groupdict()
                content_hash = utils.get_content_hash(act_soup, [{'name':'div','id':re.compile(re.compile(r'ementa.*'))}])
                base_path = f'{publication_date["year"]}/{publication_date["month"]}/{publication_date["day"]}_{act_id}_{content_hash}'
                to_download.append(base.Content(
                    content=str(act_soup.find('div',class_='secaoFormulario')),
                    dest=f'{base_path}.html',
                    content_type='text/html'))
                
                pdf_link = [link for link in act_soup.find_all('a') if link.next.next=='Carregar documento']
                if pdf_link:
                    pdf_link = pdf_link.pop()    
                    from io import BytesIO
                    from zipfile import ZipFile
                    from PyPDF2 import PdfFileMerger, PdfFileReader

                    PATTERN = re.compile(r".*replace\(\'(.*?)\'\)")
                    new = PATTERN.search(pdf_link['href']).group(1)
                    url = f"{BASE_URL}{new}"
                    requests.get(url).content
                    resp = requests.get(url)
                    zipfile = ZipFile(BytesIO(resp.content))
                    # files = []
                    merger = PdfFileMerger()
                    for file in sorted(zipfile.namelist()):
                        merger.append(zipfile.open(file))

                    from io import BytesIO
                    b = BytesIO()
                    merger.write(b)

                    to_download.append(base.Content(
                        content=b.getvalue(),
                        dest=f'{base_path}.pdf',
                        content_type='application/pdf'))
                else:
                    logger.warn(f'Inteiro not available for {act_id}')
                yield to_download

    def act_id_from_url(self,url):
        query = urlsplit(url).query
        return dict(parse_qsl(query))['idAto']

    def fetch_act(self, act_id):
        base_url = 'http://normas.receita.fazenda.gov.br/sijut2consulta'
        aux_url = '/link.action'
        response = requests.get(f'{base_url}{aux_url}',params={
            'visao':'anotado','idAto':act_id})
        if response.status_code == 200:
            soup = utils.soup_by_content(response.text)
            pdf_link = soup.find('a', text=lambda text: text and 'pdf' in text)
            pdf_url = f'{base_url}/{pdf_link["href"]}' if pdf_link else None
            return response.text,pdf_url

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
