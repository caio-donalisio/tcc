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


existing_params = {
    # "spellcheck.collateExtendedResults":"true",
    # "df":"text",
    # "hl":"off",
    # "echoParams":"ALL",
    # "fl":"*,score",
    # "spellcheck.maxCollations":"3",
    # "mlt.qf":"\n        _texto\n       ",
    # "spellcheck.maxCollationTries":"5",
    # "title":"SERPRO - BigData - Busca para Ciencia de Dados",
    # "mlt.count":"3",
    # "v.template":"browse",
    # "defType":"edismax",
    # "spellcheck.maxResultsForSuggest":"5",
    # "qf":"\n        _texto\n\n       ",
    # "wt":"json",
    # "stopwords":"true",
    # "mm":"100%",
    # "q.alt":"*:*",
    # "facet.field":[
    #     "turma_s",
    #     "camara_s",
    #     "secao_s",
    #     "materia_s",
    #     "nome_relator_s",
    #     "ano_sessao_s",
    #     "ano_publicacao_s",
    #     "_nomeorgao_s",
    #     "_turma_s",
    #     "_materia_s",
    #     "_recurso_s",
    #     "_julgamento_s",
    #     "_ementa_assunto_s",
    #     "_tiporecurso_s",
    #     "_processo_s",
    #     "_resultadon2_s",
    #     "_orgao_s",
    #     "_recorrida_s",
    #     "_tipodocumento_s",
    #     "_nomerelator_s",
    #     "_recorrente_s",
    #     "decisao_s"
    #     ],
    # "v.layout":"layout",
    #"rows":10,#NUMERO DE RESULTADOS POR BUSCA
    #"sort":"id asc",
    #"start":0,
    # "spellcheck.alternativeTermCount":"2",
    # "spellcheck.extendedResults":"false",
    #"q":"",#TERMO DE BUSCA
    # "fq":[
        #e.g. turma_s:"Quarta+Câmara"
        #e.g. ano_sessao_s:"2014"
        
        # '_version_',
        # 'ano_publicacao_s',
        # 'ano_sessao_s',
        # 'anomes_publicacao_s',
        # 'anomes_sessao_s',
        # 'arquivo_indexado_s',
        # 'atualizado_anexos_dt',
        # 'camara_s',
        # 'conteudo_id_s',
        # 'conteudo_txt',
        # 'decisao_txt',
        # 'dt_index_tdt',
        # 'dt_publicacao_tdt',
        # 'dt_registro_atualizacao_tdt',
        # 'dt_sessao_tdt',
        # 'ementa_s',
        # 'id',
        # 'materia_s',
        # 'nome_arquivo_pdf_s',
        # 'nome_arquivo_s',
        # 'nome_relator_s',
        # 'numero_decisao_s',
        # 'numero_processo_s',
        # 'score',
        # 'secao_s',
        # 'sem_conteudo_s',
        # 'turma_s'
    #    ],
    # "facet.limit":"15",
    # "spellcheck":"on",
    # "mlt.fl":"ementa_s,decisao_s",
    # "facet.mincount":"1",
    # "spellcheck.count":"5",
    # "facet":"on",
    # "spellcheck.collate":"true",
    # "rid":"-1489146"
    }


logger = logger_factory('carf')


class CARFClient:

    def __init__(self):
        self.url = 'https://acordaos.economia.gov.br/solr/acordaos2/browse?'
        

    @utils.retryable(max_retries=3)
    def count(self,filters):
        '''
        Conta registros existentes em uma dada busca
        '''
        result = self.fetch(filters,page=1)
        return result['response']['numFound']

    @utils.retryable(max_retries=3)
    def fetch(self, filters, page=1):
        '''
        Monta as buscas a serem realizadas
        '''
        try:

            items_per_page = filters.get('rows')

            params = {
                **filters,
                **{'start': (page - 1) * items_per_page}
            }
            
            return requests.get(self.url,
                                params=params,
                                verify=False).json()

        except Exception as e:
            logger.error(f"page fetch error params: {filters}")
            raise e


class CARFCollector(base.ICollector):

    def __init__(self, client, filters):
        self.client = client
        self.filters = filters

    def count(self):
        '''
        Determina quantos processos existem
        '''
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
        '''Linhas de registro devolvidos pelo chunk'''
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
            base.Content(
                content=json.dumps(record), 
                dest=dest_record,
                content_type='application/json'
                )
            ,
            base.ContentFromURL(
                src=report_url, 
                dest=dest_report,
                content_type='text/html'
                )
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

    #Parâmetros fq e seus códigos correspondentes de busca
    fq = {
        'ano_publicacao':'ano_publicacao_s',
        'ano_sessao':'ano_sessao_s',
        'turma':'turma_s',
        'camara':'camara_s',
        'secao':'secao_s',
        'materia':'materia_s',
        'nome_relator':'nome_relator_s',
        'id':'id'
        }
    
    fq_keys = (key for key in kwargs if kwargs[key] and key in fq)

    query_params = {
        'sort':kwargs.get('sort'),
        'rows':kwargs.get('rows'),
        'wt':'json',
        'fq':' AND '.join([f'{fq[key]}:"{kwargs[key]}"'for key in fq_keys]),
        'q':kwargs.get('search_term'),
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
@click.option('--rows'      ,    default=10,       help='Number of results per page')
@click.option('--sort'      ,    default='id asc', help='Sorting parameter')
@click.option('--ano-sessao',    default=None,     help='Session year')
@click.option('--ano-publicacao',default=None,     help='Publication year')
@click.option('--turma',         default=None,     help='Turma')
@click.option('--camara',        default=None,     help='Camara')
@click.option('--secao',         default=None,     help='Seção')
@click.option('--materia',       default=None,     help='Matéria')
@click.option('--nome-relator',  default=None,     help='Nome do relator')
@click.option('--search-term',   default='',       help='Search terms')
@click.option('--id',            default=None,     help='Process ID')
@click.option('--output-uri',    default=None,     help='Output URI (e.g. gs://bucket_name')
@click.option('--enqueue'   ,    default=False,    help='Enqueue for a worker'  , is_flag=True)
def carf_command(**kwargs):    
    if kwargs.get('enqueue'):
        task_id = carf_task.delay(**kwargs)
        print(f"task {task_id} sent with params {str(kwargs)}")
    else:
        carf_task(**kwargs)
