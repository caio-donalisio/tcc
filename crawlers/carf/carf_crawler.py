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

logger = logger_factory('carf')


class CARFClient:

    def __init__(self):
        self.url = 'https://acordaos.economia.gov.br/solr/acordaos2/browse?'


    @utils.retryable(max_retries=3)
    def count(self,filters):
        result = self.fetch(filters,page=1)
        return result['response']['numFound']

    @utils.retryable(max_retries=3)
    def fetch(self, filters, page=1):

        items_per_page = filters.get('rows')

        params = {
            **filters,
            **{'start': (page - 1) * items_per_page}
        }

        return requests.get(self.url,
                            params=params,
                            verify=False).json()



class CARFCollector(base.ICollector):

    def __init__(self, client, filters):
        self.client = client
        self.filters = filters

    def count(self):
        return self.client.count(self.filters)

    def chunks(self):
        total = self.count()
        pages = math.ceil(total/self.filters.get('rows'))

        for page in range(1, pages + 1):
            yield CARFChunk(
                keys={
                    **self.filters  , **{'page': page}
                },
                prefix='',
                filters=self.filters,
                page=page,
                client=self.client

            )


class CARFChunk(base.Chunk):

    def __init__(self, keys, prefix, filters, page,client):
        super(CARFChunk, self).__init__(keys, prefix)
        self.filters  = filters
        self.page = page
        self.client = client


    def rows(self):
        result = self.client.fetch(self.filters,self.page)
        for record in result['response']['docs']:

            session_at = pendulum.parse(record['dt_sessao_tdt'])

            record_id = record['id']
            base_path   = f'{session_at.year}/{session_at.month:02d}'
            report_id,_ = record['nome_arquivo_pdf_s'].split('.')
            dest_record = f"{base_path}/doc_{record_id}_{report_id}.json"

            report_url = f'https://acordaos.economia.gov.br/acordaos2/pdfs/processados/{report_id}.pdf'
            dest_report = f"{base_path}/doc_{record_id}_{report_id}.pdf"

            yield [
            base.Content(content=json.dumps(record),dest=dest_record,
                content_type='application/json'),
            base.ContentFromURL(src=report_url,dest=dest_report,
                content_type='application/pdf')
            ]

@celery.task(queue='crawlers.carf', default_retry_delay=5 * 60,
            autoretry_for=(BaseException,))
def carf_task(**kwargs):
    import utils
    setup_cloud_logger(logger)

    from logutils import logging_context

    with logging_context(crawler='carf'):
        output = utils.get_output_strategy_by_path(path=kwargs.get('output_uri'))
        logger.info(f'Output: {output}.')

        start_date = kwargs.get('start_date') + ' 00:00:00'
        end_date = kwargs.get('end_date') + ' 23:59:59'

        date_format = 'YYYY-MM-DD HH:mm:ss'

        start_date = pendulum.from_format(start_date,date_format).to_iso8601_string()
        end_date = pendulum.from_format(end_date,date_format).to_iso8601_string()
        time_interval = f'dt_sessao_tdt:[{start_date} TO {end_date}]'

        query_params = {
            'sort':'id asc',
            'rows':10,
            'wt':'json',
            'fq':time_interval,
            }

        collector = CARFCollector(client=CARFClient(), filters=query_params)
        handler   = base.ContentHandler(output=output)
        snapshot = base.Snapshot(keys=query_params)

        base.get_default_runner(
            collector=collector,
            output=output,
            handler=handler,
            logger=logger,
            max_workers=8) \
            .run(snapshot=snapshot)

@cli.command(name='carf')
@click.option('--start-date',
  default=utils.DefaultDates.BEGINNING_OF_YEAR_OR_SIX_MONTHS_BACK.strftime("%Y-%m-%d"),
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
def carf_command(**kwargs):
  if kwargs.get('enqueue'):
    if kwargs.get('split_tasks'):
      start_date = pendulum.parse(kwargs.get('start_date'))
      end_date = pendulum.parse(kwargs.get('end_date'))
      for start, end in utils.timely(start_date, end_date, unit=kwargs.get('split_tasks'), step=1):
        task_id = carf_task.delay(
          start_date=start.to_date_string(),
          end_date=end.to_date_string(),
          output_uri=kwargs.get('output_uri'))
        print(f"task {task_id} sent with params {start.to_date_string()} {end.to_date_string()}")
    else:
      carf_task.delay(**kwargs)
  else:
    carf_task(**kwargs)
