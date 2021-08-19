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



DEFAULT_HEADERS = {
  'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
                        ' AppleWebKit/537.36 (KHTML, like Gecko)'
                        ' Chrome/92.0.4515.131 Safari/537.36 Edg/92.0.902.67')
}


logger = logger_factory('tst')


class TSTClient:

    def __init__(self):
        self.url ='https://jurisprudencia-backend.tst.jus.br/rest/pesquisa-textual'

    @utils.retryable(max_retries=3)
    def count(self,filters):
        result = self.fetch(filters,page=1)
        return result['totalRegistros']

    @utils.retryable(max_retries=3)
    def fetch(self, filters, page=1):
        try:

            search_url = '/'.join([
                self.url,
                str(1 + ( (page - 1) * filters.get('rows') )),
                str(filters.get('rows')),
            ])

            post_data = {
            "ou": None,
            "e": None,
            "termoExato": "",
            "naoContem": None,
            "ementa": None,
            "dispositivo": None,
            "numeracaoUnica": {
                "numero": None,
                "digito": None,
                "ano": None,
                "orgao": "5",
                "tribunal": None,
                "vara": None
            },
            "orgaosJudicantes": [],
            "ministros": [],
            "convocados": [],
            "classesProcessuais": [],
            "indicadores": [],
            "tiposDecisoes": [],
            "tipos": ["ACORDAO"],
            "orgao": "TST",
            "julgamentoInicial": filters.get('start_date'),
            "julgamentoFinal": filters.get('end_date')
            }
            
            #sleep(0.5*random())
            return requests.post(search_url,
                json=post_data,
                headers=DEFAULT_HEADERS).json()

        except Exception as e:
            logger.error(f"page fetch error params: {filters}")
            raise e


class TSTCollector(base.ICollector):

    def __init__(self, client, filters):
        self.client = client
        self.filters = filters

    def count(self):
        return self.client.count(self.filters)

    def chunks(self):
        total = self.count()
        pages = math.ceil(total/self.filters.get('rows'))

        for page in range(1, pages + 1):
            yield TSTChunk(
                keys={
                    **self.filters  , **{'page': page}
                },
                prefix='',
                filters=self.filters,
                page=page,
                client=self.client

            )

class TSTHandler(base.ContentHandler):
    
    @utils.retryable(max_retries=9)
    def handle(self, event):
        if isinstance(event, base.ContentFromURL):
            self.download(event)
        else:
            super().handle(event)

    @utils.retryable(max_retries=3, sleeptime=5., ignore_if_exceeds=True)
    def download(self, event):
        if self.output.exists(event.dest):
            return

        try:
            response = requests.get(event.src,
                allow_redirects=True,
                verify=False)

            if response.status_code == 404:
                return

            if event.content_type:
                dest = event.dest
                content_type = event.content_type

            else:
                content_type = response.headers['Content-type'].split(';')[0].strip()
                dest = f'{event.dest}{guess_extension(content_type)}'
                if 'rtf' in content_type:
                    return
                    
            if response.status_code == 200:
                self.output.save_from_contents(
                    filepath=dest,
                    contents=response.content,
                    content_type=content_type)

        except requests.exceptions.ChunkedEncodingError:
            logger.warn(
            f"Got a ChunkedEncodingError when fetching {event.src} -- will retry.")
            return 


class TSTChunk(base.Chunk):

    def __init__(self, keys, prefix, filters, page,client):
        super(TSTChunk, self).__init__(keys, prefix)
        self.filters  = filters
        self.page = page
        self.client = client
        

    def rows(self):
        result = self.client.fetch(self.filters,self.page)
        
        base_pdf_report_url = 'http://aplicacao5.tst.jus.br/consultaDocumento/acordao.do?'
        base_html_report_url = 'https://jurisprudencia-backend.tst.jus.br/rest/documentos'

        for record in result['registros']:
            
            session_at = pendulum.parse(record['registro']['dtaJulgamento'])

            record_id = record['registro']['id']                
            base_path   = f'{session_at.year}/{session_at.month:02d}'
            
            dest_record = f"{base_path}/doc_{record_id}.json"

            html_report_url = f'{base_html_report_url}/{record_id}'
            dest_html_report = f'{base_path}/doc_{record_id}.html'

            files_to_download = []

            if 'dtaPublicacao' in record['registro'].keys():

                publication_at =  pendulum.parse(record['registro']['dtaPublicacao'])
                
                params = {
                    'anoProcInt':record['registro']['anoProcInt'],
                    'numProcInt':record['registro']['numProcInt'],
                    'dtaPublicacaoStr':publication_at.format('DD/MM/YYYY') + '%2007:00:00',
                    'nia':record['registro']['numInterno'],
                    'origem':'documento'
                    }
                    
                rtf_params = '&'.join(f'{k}={v}' for k,v in params.items())
                rtf_report_url = base_pdf_report_url + rtf_params
                dest_rtf_report = f'{base_path}/doc_{record_id}.rtf'
                files_to_download.append(base.ContentFromURL(src=rtf_report_url,dest=dest_rtf_report,
                    content_type='application/rtf'))

                pdf_params = '&'.join(f'{k}={v}' for k,v in params.items() if k not in ['origem'])
                pdf_report_url = base_pdf_report_url + pdf_params
                dest_pdf_report = f'{base_path}/doc_{record_id}'
                files_to_download.append(base.ContentFromURL(src=pdf_report_url,dest=dest_pdf_report))


            files_to_download.append(base.Content(content=json.dumps(record),dest=dest_record,
                content_type='application/json'))

            files_to_download.append(base.ContentFromURL(src=html_report_url,dest=dest_html_report,
                content_type='text/html'))

            sleep(random()+1.13)
            
            yield files_to_download
            


@celery.task(queue='crawlers.tst', default_retry_delay=5 * 60,
            autoretry_for=(BaseException,))
def tst_task(**kwargs):
    import utils
    setup_cloud_logger(logger)

    from logutils import logging_context

    with logging_context(crawler='tst'):
        output = utils.get_output_strategy_by_path(path=kwargs.get('output_uri'))
        logger.info(f'Output: {output}.')

        query_params = {
            'rows':20,
            'start_date':kwargs.get('start_date'),
            'end_date':kwargs.get('end_date')
            }
                        
        collector = TSTCollector(client=TSTClient(), filters=query_params)
        #handler = base.ContentHandler(output=output)
        handler   = TSTHandler(output=output) 
        snapshot = base.Snapshot(keys=query_params)

        base.get_default_runner(
            collector=collector, 
            output=output, 
            handler=handler, 
            logger=logger, 
            max_workers=8) \
            .run(snapshot=snapshot)

@cli.command(name='tst')
@click.option('--start-date',    prompt=True,      help='Format YYYY-MM-DD.')
@click.option('--end-date'  ,    prompt=True,      help='Format YYYY-MM-DD.')
@click.option('--output-uri',    default=None,     help='Output URI (e.g. gs://bucket_name')
@click.option('--enqueue'   ,    default=False,    help='Enqueue for a worker'  , is_flag=True)
@click.option('--split-tasks',
  default=None, help='Split tasks based on time range (weeks, months, days, etc) (use with --enqueue)')
def tst_command(**kwargs):
  if kwargs.get('enqueue'):
    if kwargs.get('split_tasks'):
      start_date = pendulum.parse(kwargs.get('start_date'))
      end_date = pendulum.parse(kwargs.get('end_date'))
      for start, end in utils.timely(start_date, end_date, unit=kwargs.get('split_tasks'), step=1):
        task_id = tst_task.delay(
          start.to_date_string(),
          end.to_date_string(),
          kwargs.get('output_uri'))
        print(f"task {task_id} sent with params {start.to_date_string()} {end.to_date_string()}")
    else:
      tst_task.delay(**kwargs)
  else:
    tst_task(**kwargs)
