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


logger = logger_factory('scrfb')

def get_filters(start_date, end_date,**kwargs):
    return {
        'facetsExistentes': '',
        'orgaosSelecionados': '',
        'tiposAtosSelecionados': '72%3B+75%3B+73',
        'lblTiposAtosSelecionados': 'SC%3B+SCI%3B+SD',
        'ordemColuna': 'Publicacao',
        'ordemDirecao': 'DESC',
        'tipoConsulta': 'formulario',
        'tipoAtoFacet': '',
        'siglaOrgaoFacet': '',
        'anoAtoFacet': '',
        'termoBusca': 'a',
        'numero_ato': '',
        'tipoData': '2',  # DATA DE PUBLICAÇÃO
        'dt_inicio': start_date.format(SEARCH_DATE_FORMAT),
        'dt_fim': end_date.format(SEARCH_DATE_FORMAT),
        'ano_ato': '',
        'optOrdem': 'Publicacao_DESC'
    }

COURT_NAME = 'scrfb'
RESULTS_PER_PAGE = 100  # DEFINED BY THEIR SERVER
INPUT_DATE_FORMAT = 'YYYY-MM-DD'
SEARCH_DATE_FORMAT = 'DD/MM/YYYY'
NOW = pendulum.now()

class SCRFBClient:

    def __init__(self):
        self.url = 'http://normas.receita.fazenda.gov.br/sijut2consulta/'

    @utils.retryable(max_retries=3)
    def count(self,filters):
        result = self.fetch(filters)
        soup = utils.soup_by_content(result.text)
        count_tag = soup.find('ul', attrs={'class':'pagination total-regs-encontrados'})
        if count_tag:
            count = re.search(r'\s*Total de atos localizados:\s*(\d+)[\s\n].*',count_tag.text)
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
        try:
            return requests.get(
                url=f'{self.url}/consulta.action?',
                params={
                    **get_filters(**filters),
                    'p':page
                }
            )

        except Exception as e:
            logger.error(f"page fetch error params: {filters}")
            raise e


class SCRFBCollector(base.ICollector):

    def __init__(self, client, filters):
        self.client = client
        self.filters = filters

    def count(self, period=None):
        if 
        if self.filters.get('count_only'):
            return self.client.count_periods(self.filters)
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
            total = self.count(){'start_date':start,'end_date':end})
            pages = [1] if self.filters['count_only'] else range(1, 2 + total//RESULTS_PER_PAGE)
            for page in pages:
                yield SCRFBChunk(
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
class SCRFBChunk(base.Chunk):

    def __init__(self, keys, prefix, filters, page, count_only, client):
        super(SCRFBChunk, self).__init__(keys, prefix)
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
            acts = soup.find_all('tr', class_='linhaResultados')
            to_download = []
            for act in acts:
                if not act.a:
                    continue
                act_id = self.act_id_from_url(act.a['href'])
                publication_date = act.find_all('td')[3].text
                date_id = ''.join(publication_date.split('/')[::-1]).replace('/','')
                html_content,pdf_url=self.fetch_act(act_id=act_id)#,publication_date=publication_date)                
                content_id= utils.get_content_hash(
                    soup=utils.soup_by_content(html_content),
                    tag_descriptions=[{'name':'p','class_':'ementa'}])
                
                to_download.append(base.Content(
                    content=html_content,
                    dest=utils.get_filepath(publication_date, f'{date_id}_{act_id}_{content_id}','html'),
                    content_type='text/html'))

                if pdf_url:
                    to_download.append(base.ContentFromURL(
                        src=pdf_url,
                        dest= utils.get_filepath(publication_date, f'{date_id}_{act_id}_{content_id}','pdf'),
                        content_type='application/pdf'))
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

@celery.task(queue='crawlers.scrfb', default_retry_delay=5 * 60,
            autoretry_for=(BaseException,))
def scrfb_task(**kwargs):
    import utils
    setup_cloud_logger(logger)

    from logutils import logging_context

    with logging_context(crawler='scrfb'):
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

        collector = SCRFBCollector(client=SCRFBClient(), filters=query_params)
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
def scrfb_command(**kwargs):
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
        task_id = scrfb_task.delay(
          start_date=start.to_date_string(),
          end_date=end.to_date_string(),
          output_uri=kwargs.get('output_uri'))
        print(f"task {task_id} sent with params {start.to_date_string()} {end.to_date_string()}")
    else:
      scrfb_task.delay(**kwargs)
  else:
    scrfb_task(**kwargs)
