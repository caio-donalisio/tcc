import base
import math
import pendulum
import celery
import utils
from logconfig import logger_factory, setup_cloud_logger
import click
from app import cli, celery
import requests
from bs4 import BeautifulSoup
import re

DEFAULT_HEADERS = {
     'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:88.0) Gecko/20100101 Firefox/88.0',
     'Accept': '*/*',
     'Accept-Encoding':'gzip, deflate',
     'Referer': 'http://web.trf3.jus.br/base-textual'
            }

TRF3_DATE_FORMAT = 'DD/MM/YYYY'
DATE_PATTERN = re.compile(r'\d{2}/\d{2}/\d{4}')
FILES_PER_PAGE = 50

def get_post_data(filters):
    return {
        'txtPesquisaLivre': '',
        'chkMostrarLista': 'on',
        'numero': '',
        'magistrado': 0,
        'data_inicial': pendulum.parse(filters.get('start_date')).format(TRF3_DATE_FORMAT),
        'data_final': pendulum.parse(filters.get('end_date')).format(TRF3_DATE_FORMAT),
        'data_tipo': 1,
        'classe': 0,
        'numclasse': '',
        'orgao': 0,
        'ementa': '',
        'indexacao': '',
        'legislacao': '',
        'chkAcordaos': 'on',
        'hdnMagistrado': '',
        'hdnClasse': '',
        'hdnOrgao': '',
        'hdnLegislacao': '',
        'hdnMostrarListaResumida': ''
    }


logger = logger_factory('trf3')


class TRF3Client:

    def __init__(self):
        self.session = requests.Session()

    def setup(self):
        self.session.get('http://web.trf3.jus.br/base-textual',headers=DEFAULT_HEADERS)

    @utils.retryable(max_retries=9)
    def count(self,filters):
        result = self.fetch(filters)
        soup = BeautifulSoup(result.text)
        count = soup.find('a',{'href':'/base-textual/Home/ListaResumida/1?np=0'}).text
        if count:
            return int(''.join([char for char in count if char.isdigit()]))
        else:
            return 0

    @utils.retryable(max_retries=9)
    def fetch(self, filters):
        self.setup()
        post_data = get_post_data(filters)
        url = 'http://web.trf3.jus.br/base-textual/Home/ResultadoTotais'
        return self.session.post(url,
            json=post_data,
            headers=DEFAULT_HEADERS)



class TRF3Collector(base.ICollector):

    def __init__(self, client,filters):
        self.client = client
        self.filters = filters

    def count(self):
        return self.client.count(self.filters)

    def chunks(self):
        total = self.count()
        pages = math.ceil(total/FILES_PER_PAGE)

        for page in range(1, pages + 2):
            yield TRF3Chunk(
                keys={**self.filters , **{'page': page}},
                prefix='',
                page=page,
                total=total,
                client=self.client,
            )


class TRF3Chunk(base.Chunk):

    def __init__(self, keys, prefix, page,total,client):
        super(TRF3Chunk, self).__init__(keys, prefix)
        self.page = page
        self.total=total
        self.client = client

    @utils.retryable(max_retries=9)
    def rows(self):
        BASE_INTEIRO_URL = 'http://web.trf3.jus.br'

        for proc_number in range(
            1 + ((self.page - 1) * FILES_PER_PAGE),
            1 + min(self.total,((self.page) * FILES_PER_PAGE))
            ):
            response = self.client.session.get(f'http://web.trf3.jus.br/base-textual/Home/ListaColecao/9?np={proc_number}',headers=DEFAULT_HEADERS)
            soup = BeautifulSoup(response.text,features='html5lib')
            
            pub_date_div = soup.find('div',text='Data da Publicação/Fonte ')
            pub_date, = DATE_PATTERN.findall(pub_date_div.next_sibling.next_sibling.text)
            
            url_page_acordao = soup.find('a',{'title':'Exibir a íntegra do acórdão.'}).get('href')
            page_acordao = requests.get(url_page_acordao,headers=DEFAULT_HEADERS)
            page_acordao_soup = BeautifulSoup(page_acordao.text,features='html5lib')

            link_to_inteiro = page_acordao_soup.find('a',text=pub_date) or \
                              page_acordao_soup.find('a',{'name':'Pje'})
            url_acordao_inteiro = link_to_inteiro.get('href')
            acordao_inteiro = requests.get(f'http://web.trf3.jus.br{url_acordao_inteiro}',headers=DEFAULT_HEADERS)

            data_julg_div = soup.find('div',text='Data do Julgamento ')
            session_at = data_julg_div.next_sibling.next_sibling.text.strip()
            session_at = pendulum.from_format(session_at,TRF3_DATE_FORMAT)

            processo_text = soup.find('h4',text='Processo').next_sibling.next_sibling.text.strip()
            processo_num = ''.join(char for char in processo_text if char.isdigit())

            dest_path = f'{session_at.year}/{session_at.month:02d}/{session_at.day:02d}_{processo_num}.html'
            dest_path_completo = f'{session_at.year}/{session_at.month:02d}/{session_at.day:02d}_{processo_num}_INTEIRO.html'

            yield [
                base.Content(content=response.text, dest=dest_path,content_type='text/html'),#content_type='text/html'),
                base.Content(content=acordao_inteiro.text,dest = dest_path_completo,content_type='text/html')
                    ]


@celery.task(queue='crawlers.trf3', default_retry_delay=5 * 60,
            autoretry_for=(BaseException,))
def trf3_task(**kwargs):
    import utils
    setup_cloud_logger(logger)

    from logutils import logging_context

    with logging_context(crawler='trf3'):
        output = utils.get_output_strategy_by_path(path=kwargs.get('output_uri'))
        logger.info(f'Output: {output}.')

        query_params = {
            'start_date':kwargs.get('start_date'),
            'end_date':kwargs.get('end_date')
            }

        collector = TRF3Collector(client=TRF3Client(), filters=query_params)
        handler = base.ContentHandler(output=output)
        snapshot = base.Snapshot(keys=query_params)

        base.get_default_runner(
            collector=collector,
            output=output,
            handler=handler,
            logger=logger,
            max_workers=8) \
            .run(snapshot=snapshot)

@cli.command(name='trf3')
@click.option('--start-date',    prompt=True,      help='Format YYYY-MM-DD.')
@click.option('--end-date'  ,    prompt=True,      help='Format YYYY-MM-DD.')
@click.option('--output-uri',    default=None,     help='Output URI (e.g. gs://bucket_name')
@click.option('--enqueue'   ,    default=False,    help='Enqueue for a worker'  , is_flag=True)
@click.option('--split-tasks',
  default=None, help='Split tasks based on time range (weeks, months, days, etc) (use with --enqueue)')
def trf3_command(**kwargs):
  if kwargs.get('enqueue'):
    if kwargs.get('split_tasks'):
      start_date = pendulum.parse(kwargs.get('start_date'))
      end_date = pendulum.parse(kwargs.get('end_date'))
      for start, end in utils.timely(start_date, end_date, unit=kwargs.get('split_tasks'), step=1):
        task_id = trf3_task.delay(
          start_date=start.to_date_string(),
          end_date=end.to_date_string(),
          output_uri=kwargs.get('output_uri'))
        print(f"task {task_id} sent with params {start.to_date_string()} {end.to_date_string()}")
    else:
      trf3_task.delay(**kwargs)
  else:
    trf3_task(**kwargs)
