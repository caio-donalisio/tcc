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
from random import random
from time import sleep
from mimetypes import guess_extension
from bs4 import BeautifulSoup


DEFAULT_HEADERS = {
  'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
                        ' AppleWebKit/537.36 (KHTML, like Gecko)'
                        ' Chrome/92.0.4515.131 Safari/537.36 Edg/92.0.902.67')
}
DEFAULT_DATE_FORMAT = 'YYYY-MM-DD'
TRF3_DATE_FORMAT = 'DD/MM/YYYY'

logger = logger_factory('trf3')


class TRF3Client:

    def __init__(self):
        #self.url ='https://jurisprudencia-backend.trf3.jus.br/rest/pesquisa-textual'
        self.url = 'http://web.trf3.jus.br/base-textual/Home/ResultadoTotais'

    @utils.retryable(max_retries=9)
    def count(self,filters):
        result = self.fetch(filters,page=1)
        soup = BeautifulSoup(result)
        text = soup.find('a',{'href':'/base-textual/Home/ListaResumida/1?np=0'}).text
        print(text)
        return result['totalRegistros']

    @utils.retryable(max_retries=9)
    def fetch(self, filters, page=1):
        post_data = {
        'txtPesquisaLivre': '',
        'chkMostrarLista': 'on',
        'numero': '',
        'magistrado': 0,
        'data_inicial': filters.get('start_date').format(TRF3_DATE_FORMAT),
        'data_final': filters.get('end_date').format(TRF3_DATE_FORMAT),
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

        #sleep(0.5*random())
        return requests.post(self.url,
            json=post_data,
            headers=DEFAULT_HEADERS)



class TRF3Collector(base.ICollector):

    def __init__(self, client, filters):
        self.client = client
        self.filters = filters

    def count(self):
        return self.client.count(self.filters)

    def chunks(self):
        total = self.count()
        pages = math.ceil(total/10)

    
        for page in range(1, pages + 1):
            yield TRF3Chunk(
                keys={**self.filters , **{'page': page}},
                prefix='',
                filters=self.filters,
                page=page,
                client=self.client
            )

class TRF3Chunk(base.Chunk):

    def __init__(self, keys, prefix, filters, page,client):
        super(TRF3Chunk, self).__init__(keys, prefix)
        self.filters  = filters
        self.page = page
        self.client = client

    def rows(self):
        result = self.client.fetch(self.filters,self.page)

        for record in result['registros']:

            sleep(random()+1.13)


            yield base.Content('aaa','05/05/bla.html')



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
            'start_date':pendulum.parse(kwargs.get('start_date')),
            'end_date':pendulum.parse(kwargs.get('end_date'))
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
