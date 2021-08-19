import base
import math
import json
import pendulum
import celery
import utils
from logconfig import logger_factory, setup_cloud_logger
import click
from app import cli, celery
import requests
import urllib
from browsers import FirefoxBrowser
from selenium.webdriver.common.by import By
import time

logger = logger_factory('tjrs')

def force_int(text):
    try:
        return int(text)
    except ValueError:
        return text

SOURCE_DATE_FORMAT='DD/MM/YYYY'
DEFAULT_HEADERS = {
  'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
                        ' AppleWebKit/537.36 (KHTML, like Gecko)'
                        ' Chrome/92.0.4515.131 Safari/537.36 Edg/92.0.902.67'),
    'Cookie':'cookielawinfo-checkbox-analytics=no; cookielawinfo-checkbox-functional=no; cookielawinfo-checkbox-necessary=yes; cookielawinfo-checkbox-performance=no; cookielawinfo-checkbox-advertisement=no; cookielawinfo-checkbox-others=no; _ga=GA1.3.1484575181.1629204768; _gid=GA1.3.1734249402.1629204768; __utmz=245208256.1629215859.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); __utma=245208256.1484575181.1629204768.1629215859.1629223977.2; PHPSESSID=pkaju47lkv1hj9dirlgqom44eq; PHPINTER-PROD=.phpinter-prod-02; WORDPRESS=.wordpress-dmz-3'
                        }


class TJRSClient:

    def __init__(self):
        self.url = 'https://www.tjrs.jus.br/buscas/jurisprudencia/ajax.php?'


    @utils.retryable(max_retries=3)
    def count(self,filters):
        result = self.fetch(filters,page=1)
        return result['response']['numFound']

    # def setup(self):
    #     try:
    #         url = f'https://www.tjrs.jus.br/novo/buscas-solr/?aba=jurisprudencia&q=&conteudo_busca=ementa_completa'
    #         browser = self.browser
    #         browser.get(url)
    #         self.session_id  = browser.get_cookie('JSESSIONID')
    #         self.cookie_id = browser.get_cookie('juridico')
    #         self.headers = {
    #         'cookie': f'JSESSIONID={self.session_id};juridico={self.cookie_id}',
    #         **DEFAULT_USER_AGENT
    #         }

        # except Exception as e:
        #     browser.quit()
        #     raise e
    
    def get_cookie(self, cookie_name):
        value = None
        cookies = self.driver.get_cookies()
        for cookie in cookies:
            if cookie.get('name') == cookie_name:
                value = cookie['value']
        if cookie is None:
            raise Exception(f'Cookie not found: {cookie_name}')
        return value


    @utils.retryable(max_retries=3)
    def fetch(self, filters, page=1):
        try:
            if isinstance(filters['parametros'],str):
                filters_aux = urllib.parse.parse_qs(filters['parametros'])
                filters_aux = {k:v[0] for k,v in filters_aux.items()}
                filters_aux = {k:force_int(v) for k,v in filters_aux.items()}
                filters_aux['pagina_atual']  = page
                filters['parametros'] = filters_aux

            filters['parametros'] = urllib.parse.urlencode(filters['parametros'],quote_via=urllib.parse.quote)

            return requests.post(self.url,
                                data=filters,
                                headers=DEFAULT_HEADERS
                                ).json()

        except Exception as e:
            logger.error(f"page fetch error params: {filters}")
            raise e


class TJRSCollector(base.ICollector):

    def __init__(self, client, filters):
        self.client = client
        self.filters = filters

    def count(self):
        return self.client.count(self.filters)

    def chunks(self):
        total = self.count()
        pages = math.ceil(total/10)

        for page in range(1, pages + 1):
            yield TJRSChunk(
                keys={
                    **self.filters  , **{'page': page}
                },
                prefix='',
                filters=self.filters,
                page=page,
                client=self.client

            )


class TJRSChunk(base.Chunk):

    def __init__(self, keys, prefix, filters, page,client):
        super(TJRSChunk, self).__init__(keys, prefix)
        self.filters  = filters
        self.page = page
        self.client = client
        self.browser = FirefoxBrowser(headers=DEFAULT_HEADERS)#,headless=False)


    def rows(self):
        result = self.client.fetch(self.filters,self.page)
        for n,record in enumerate(result['response']['docs']):
            
            to_download = []

            browser = self.browser
            browser.driver.implicitly_wait(10)

            session_at = pendulum.parse(record['data_julgamento'])
            base_path   = f'{session_at.year}/{session_at.month:02d}'

            codigo = record['cod_documento']
            ano = record['ano_julgamento']
            numero = record['numero_processo']

            
            dest_record = f"{base_path}/doc_{numero}_{codigo}.json"

            to_download.append(base.Content(content=json.dumps(record),dest=dest_record,
                content_type='application/json'),)

            browser.get('https://www.tjrs.jus.br/buscas/jurisprudencia/?conteudo_busca=ementa_completa&q_palavra_chave=&aba=jurisprudencia&q=&conteudo_busca=ementa_completa')

            browser.fill_in(field_id='filtroNumeroProcesso',value=numero)
            browser.fill_in(field_id='data_julgamento_de',value=session_at.format(SOURCE_DATE_FORMAT))
            browser.fill_in(field_id='data_julgamento_ate',value=session_at.format(SOURCE_DATE_FORMAT))
            browser.driver.find_element_by_class_name('button-buscar-adicional').click()
            
            if browser.driver.find_element_by_css_selector("[title='exibir doc']"):
                report_url=f'https://www.tjrs.jus.br/site_php/consulta/download/exibe_documento_att.php?numero_processo={numero}&ano={ano}&codigo={codigo}'
                dest_report = f"{base_path}/doc_{numero}_{codigo}.doc"
                to_download.append(base.ContentFromURL(src=report_url,dest=dest_report,
                    content_type='application/doc'))

            if record['documento_tiff']:
                pass

            if browser.driver.find_element_by_css_selector("[title='exibir html']"):
                browser.driver.find_element_by_class_name('exibir_html').click()
                html_content = browser.page_source()
                browser.back()
                html_dest = f"{base_path}/doc_{numero}_{codigo}.html"
                to_download.append(base.Content(content=html_content,dest=html_dest,
                    content_type='text/html'))
            
            yield to_download

@celery.task(queue='crawlers.tjrs', default_retry_delay=5 * 60,
            autoretry_for=(BaseException,))
def tjrs_task(**kwargs):
    import utils
    setup_cloud_logger(logger)

    from logutils import logging_context

    with logging_context(crawler='tjrs'):
        output = utils.get_output_strategy_by_path(path=kwargs.get('output_uri'))
        logger.info(f'Output: {output}.')

        start_date = pendulum.parse(kwargs.get('start_date')).format(SOURCE_DATE_FORMAT)
        end_date = pendulum.parse(kwargs.get('end_date')).format(SOURCE_DATE_FORMAT)

        filters = {
        'action': 'consultas_solr_ajax',
        'metodo': 'buscar_resultados',
        'parametros':{'aba':'jurisprudencia',
            'realizando_pesquisa':1,
            'pagina_atual':1,
            'q_palavra_chave':'',
            'conteudo_busca':'ementa_completa',
            'filtroComAExpressao':'',
            'filtroComQualquerPalavra':'',
            'filtroSemAsPalavras':'',
            'filtroTribunal':-1,
            'filtroRelator':-1,
            'filtroOrgaoJulgador':-1,
            'filtroTipoProcesso':-1,
            'filtroClasseCnj':-1,
            'assuntoCnj':-1,
            'filtroReferenciaLegislativa':'',
            'filtroJurisprudencia':'',
            'filtroComarcaOrigem':'',
            'filtroAssunto':'',
            'data_julgamento_de':f'{start_date}',
            'data_julgamento_ate':f'{end_date}',
            'filtroNumeroProcesso':'',
            'data_publicacao_de':'',
            'data_publicacao_ate':'',
            'filtroacordao':'acordao',
            'wt':'json',
            'ordem':'asc,cod_documento%20asc,numero_processo%20asc',
            'start':0
            }
            }

        collector = TJRSCollector(client=TJRSClient(), filters=filters)
        handler   = base.ContentHandler(output=output)
        snapshot = base.Snapshot(keys=filters)

        base.get_default_runner(
            collector=collector,
            output=output,
            handler=handler,
            logger=logger,
            max_workers=8) \
            .run(snapshot=snapshot)

@cli.command(name='tjrs')
@click.option('--start-date',    prompt=True,      help='Format YYYY-MM-DD.')
@click.option('--end-date'  ,    prompt=True,      help='Format YYYY-MM-DD.')
@click.option('--output-uri',    default=None,     help='Output URI (e.g. gs://bucket_name')
@click.option('--enqueue'   ,    default=False,    help='Enqueue for a worker'  , is_flag=True)
@click.option('--split-tasks',
  default=None, help='Split tasks based on time range (weeks, months, days, etc) (use with --enqueue)')
def tjrs_command(**kwargs):
  if kwargs.get('enqueue'):
    if kwargs.get('split_tasks'):
      start_date = pendulum.parse(kwargs.get('start_date'))
      end_date = pendulum.parse(kwargs.get('end_date'))
      for start, end in utils.timely(start_date, end_date, unit=kwargs.get('split_tasks'), step=1):
        task_id = tjrs_task.delay(
          start_date=start.to_date_string(),
          end_date=end.to_date_string(),
          output_uri=kwargs.get('output_uri'))
        print(f"task {task_id} sent with params {start.to_date_string()} {end.to_date_string()}")
    else:
      tjrs_task.delay(**kwargs)
  else:
    tjrs_task(**kwargs)
