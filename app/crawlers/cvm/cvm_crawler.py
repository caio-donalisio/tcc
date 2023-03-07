# from jmespath import search
import base
import math
import pendulum
import celery
import utils
from logconfig import logger_factory, setup_cloud_logger
import click
from app import cli, celery
import requests

logger = logger_factory('cvm')

DATE_FORMAT = 'DD/MM/YYYY'
BOUND_DATE_FORMAT = 'MM/DD/YYYY'
RESULTS_PER_PAGE = 10
DEFAULT_HEADERS = headers = {
    'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:99.0) Gecko/20100101 Firefox/99.0',
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.5',
    # 'Accept-Encoding': 'gzip, deflate, br',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'X-Requested-With': 'XMLHttpRequest',
    'Origin': 'https://conteudo.cvm.gov.br',
    'Connection': 'keep-alive',
    'Referer': 'https://conteudo.cvm.gov.br/decisoes/index.html?lastNameShow=&lastName=&filtro=todos&dataInicio=01%2F01%2F2018&dataFim=31%2F12%2F2020&buscadoDecisao=false&categoria=decisao',
    # Requests sorts cookies= alphabetically
    # 'Cookie': '_ga=GA1.3.1634521167.1650194714; _gid=GA1.3.882714664.1650194714; JSESSIONID=AB89D21C128DEDACD097B00C0F05B0BE',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
}


def get_params(start_date, end_date, page=1):
    return {
        'lastNameShow':'',
        'lastName':'',
        'filtro':'todos',
        'dataInicio': start_date.format(DATE_FORMAT),
        'dataFim': end_date.format(DATE_FORMAT),
        'categoria0':'/sancionadores/sancionador/',
        'buscado':'false',
        'contCategoriasCheck':2,
        'itensPagina':str(RESULTS_PER_PAGE),
        'ordenar':"recentes",
        'dataInicioBound':start_date.format(BOUND_DATE_FORMAT),
        'dataFimBound':end_date.format(BOUND_DATE_FORMAT),
        'listaBuscaAside':'',
        'searchPage':page,
        'tipos':'',
    }



class CVMClient:

    def __init__(self):
        # lastNameShow=&lastName=&filtro=todos&dataInicio=15%2F01%2F2019&dataFim=31%2F10%2F2021&categoria0=%2Fsancionadores%2Fsancionador%2F&buscado=false&contCategoriasCheck=2
        # self.url = 'https://conteudo.cvm.gov.br/sancionadores/index.html?'
        self.url = 'https://conteudo.cvm.gov.br/system/modules/br.com.squadra.principal/elements/resultadoDecisaoColegiado2.jsp'
        # response = requests.post('https://conteudo.cvm.gov.br/system/modules/br.com.squadra.principal/elements/resultadoDecisaoColegiado2.jsp', headers=headers, cookies=cookies, data=data)


    @utils.retryable(max_retries=3)
    def count(self,filters):
        soup = utils.soup_by_content(
            self.fetch(filters, page=1)
        )
        count = soup.find(name='div',class_='col-sm-6 no-padding').find('span').text
        count = ''.join(char for char in count if char.isdigit())
        return int(count) if count else 0

    @utils.retryable(max_retries=3)
    def fetch(self, filters, page=1):
        try:
            start_date = filters.get('start_date')
            end_date = filters.get('end_date')

            return requests.post(self.url,
                                data=get_params(start_date, end_date, page),
                                headers=DEFAULT_HEADERS,
                                verify=False).text

        except Exception as e:
            logger.error(f"page fetch error params: {filters}")
            raise e

# https://conteudo.cvm.gov.br/sancionadores/index.html?lastNameShow=&lastName=&filtro=todos&dataInicio=15%2F10%2F2019&dataFim=31%2F10%2F2021&categoria0=%2Fsancionadores%2Fsancionador%2F&buscado=false&contCategoriasCheck=2
class CVMCollector(base.ICollector):

    def __init__(self, client, filters):
        self.client = client
        self.filters = filters

    def count(self):
        return self.client.count(self.filters)

    def chunks(self):
        total = self.count()
        pages = math.ceil(total/RESULTS_PER_PAGE)

        for page in range(1, pages + 1):
            yield CVMChunk(
                keys={
                    **self.filters  , **{'page': page}
                },
                prefix='',
                filters=self.filters,
                page=page,
                client=self.client

            )


class CVMChunk(base.Chunk):

    def __init__(self, keys, prefix, filters, page,client):
        super(CVMChunk, self).__init__(keys, prefix)
        self.filters  = filters
        self.page = page
        self.client = client


    def rows(self):
        # yield
        BASE_URL = 'https://conteudo.cvm.gov.br'
        result = self.client.fetch(self.filters,self.page)
        soup = utils.soup_by_content(result)
        for link in soup.find_all('a'):
            page = requests.get(f'{BASE_URL}{link["href"]}')
            item_soup = utils.soup_by_content(page.text)
            ...
        #     session_at = pendulum.parse(record['dt_sessao_tdt'])

        #     record_id = record['id']
        #     base_path   = f'{session_at.year}/{session_at.month:02d}'
        #     report_id,_ = record['nome_arquivo_pdf_s'].split('.')
        #     dest_record = f"{base_path}/doc_{record_id}_{report_id}.json"

        #     # report_url = f'https://acordaos.economia.gov.br/acordaos2/pdfs/processados/{report_id}.pdf'
        #     dest_report = f"{base_path}/doc_{record_id}_{report_id}.pdf"

        #     yield [
        #     base.Content(content=json.dumps(record),dest=dest_record,
        #         content_type='application/json'),
        #     base.ContentFromURL(src=report_url,dest=dest_report,
        #         content_type='application/pdf')
        #     ]

@celery.task(queue='crawlers.cvm', default_retry_delay=5 * 60,
            autoretry_for=(BaseException,))
def cvm_task(**kwargs):
    import utils
    setup_cloud_logger(logger)

    from logutils import logging_context

    with logging_context(crawler='cvm'):
        output = utils.get_output_strategy_by_path(path=kwargs.get('output_uri'))
        logger.info(f'Output: {output}.')

        filters = {
            'start_date':pendulum.parse(kwargs.get('start_date')),
            'end_date':pendulum.parse(kwargs.get('end_date')),
            }        

        collector = CVMCollector(client=CVMClient(), filters=filters)
        handler   = base.ContentHandler(output=output)
        snapshot = base.Snapshot(keys=filters)

        base.get_default_runner(
            collector=collector,
            output=output,
            handler=handler,
            logger=logger,
            max_workers=8) \
            .run(snapshot=snapshot)

@cli.command(name='cvm')
@click.option('--start-date',    prompt=True,      help='Format YYYY-MM-DD.')
@click.option('--end-date'  ,    prompt=True,      help='Format YYYY-MM-DD.')
@click.option('--output-uri',    default=None,     help='Output URI (e.g. gs://bucket_name')
@click.option('--enqueue'   ,    default=False,    help='Enqueue for a worker'  , is_flag=True)
@click.option('--split-tasks',
  default=None, help='Split tasks based on time range (weeks, months, days, etc) (use with --enqueue)')
def cvm_command(**kwargs):
  if kwargs.get('enqueue'):
    if kwargs.get('split_tasks'):
      del (kwargs['enqueue'])
    split_tasks = kwargs.get('split_tasks', None)
    del (kwargs['split_tasks'])
    utils.enqueue_tasks(cvm_task, split_tasks, **kwargs)
  else:
    cvm_task(**kwargs)
