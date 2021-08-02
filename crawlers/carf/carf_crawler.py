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

def get_filters(rows=10,sort='id asc'):
    return {
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
    "wt":"json",
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
    "rows":rows,#NUMERO DE RESULTADOS POR BUSCA
    "sort":sort,
    #"start":0,
    # "spellcheck.alternativeTermCount":"2",
    # "spellcheck.extendedResults":"false",
    "q":"",#TERMO DE BUSCA
    "fq":[
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
        ],
    # "facet.limit":"15",
    # "spellcheck":"on",
    # "mlt.fl":"ementa_s,decisao_s",
    # "facet.mincount":"1",
    # "spellcheck.count":"5",
    # "facet":"on",
    # "spellcheck.collate":"true",
    # "rid":"-1489146"
    }




# importante fazer uma contagem dos processos a serem baixados
logger = logger_factory('carf')


class CARFClient:

    def __init__(self):
        self.url = 'https://acordaos.economia.gov.br/solr/acordaos2/browse?'
        

    @utils.retryable(max_retries=3)
    def count(self,filters):
        '''
        Conta registros existentes em uma dada busca
        '''
        result = self.fetch(filters,start=0)#,item_per_page=1)
        return result['response']['numFound']
        # conta registros

    @utils.retryable(max_retries=3)
    def fetch(self, filters, start=0):#, items_per_page=10):
        '''
        Monta as buscas a serem realizadas
        '''
        try:

            items_per_page = filters['rows']

            #TO DO: INCLUIR ZERO
            params = {
                **filters,
                **{'start': start + items_per_page}
            }
            
            return requests.get(self.url,
                                params=params,
                                verify=False).json()

            #EXECUTAR A QUERY
            #return []
            #return params
        
        except Exception as e:
            logger.error(f"page fetch error params: {filters}")
            raise e
        

    # @utils.retryable(max_retries=3)
    # def paginator(self, filters, items_per_page=10):
    #     item_count = self.count(filters)
    #     page_count = math.ceil(item_count / items_per_page)
    #     return Paginator(self, filters=filters, item_count=item_count, page_count=page_count,
    #         items_per_page=items_per_page)
        # retorna registros


class CARFCollector(base.ICollector):
    '''Precisa do count e do chunks'''

    def __init__(self, client, filters):
        self.client = client
        self.filters = filters

    def count(self):
        '''determina quantos processos existem'''
        return self.client.count(self.filters)

    def chunks(self):
        total = self.count()
        #pages = math.ceil(total/10)
        pages = math.ceil(total/self.filters['rows'])

        for page in range(1, pages+1):
            # filters
            # page
            yield CARFChunk(
                keys={
                    **self.filters  , **{'page': page}
                },
                # subdiretório onde guardar os blocos (deixar em branco)
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
        '''linhas de registro devolvidos pelo chunk'''
        result = self.client.fetch(self.filters,self.page)
        for record in result['response']['docs']:

                published_at = pendulum.parse(record['dt_sessao_tdt'])

                record_id = record['id']                
                base_path   = f'{published_at.year}/{published_at.month:02d}'
                report_id,_ = record['nome_arquivo_pdf_s'].split('.')
                
                
                dest_record = f"{base_path}/doc_{record_id}_{report_id}.json"
                
                report_url = f'https://acordaos.economia.gov.br/acordaos2/pdfs/processados/{report_id}.pdf'
                dest_report = f"{base_path}/doc_{record_id}_{report_id}.pdf"
            #TODO - VAI RETORNAR UM JSON
#Para essa linha, gerar uma quantidade N de arquivos (no caso o Json e o PDF)
                yield [
                    #SALVAR (não precisa chamar o codigo que faz a persistnecia - coletor só precisa indicar o conteúdo)
                base.Content(
                content=json.dumps(record), 
                dest=dest_record, #DESTINO
                content_type='application/json'
                )
                ,
            
                base.ContentFromURL(
                    src=report_url, 
                    dest=dest_report, #base_path + doc_id.pdf
                    content_type='text/html'
                    )#Só representa os dados, e o destino, não é o arquivo em si
            ]#BAIXAR O PDF
            
#COLETOR não precisa ficar se perguntando se baixou ou não
#Estrurau grosseira do crawler
@celery.task(queue='crawlers.carf', default_retry_delay=5 * 60,
            autoretry_for=(BaseException,))
def carf_task(rows,sort,ano_sessao,output_uri):
    import utils
    setup_cloud_logger(logger)

    from logutils import logging_context

    with logging_context(crawler='carf'):
        output = utils.get_output_strategy_by_path(path=output_uri)
        logger.info(f'Output: {output}.')

    #start_date, end_date =\
    #    pendulum.parse(start_date), pendulum.parse(end_date)

    query_params = {'rows':rows,
                    'sort': sort,
                    'wt':'json',
                    'fq':f'ano_sessao_s:"{ano_sessao}"'
                    }
    collector = CARFCollector(client=CARFClient(), filters=query_params)
    
    handler   = base.ContentHandler(output=output) #Handler recebe as informações do yield e de fato executa alguma coisa

    snapshot = base.Snapshot(keys=query_params)
    #Snapshot é como uma evolução do mecanismo de controle
    #O crawler se controla pelo objetivo, não de onde ele parou - 
    #Coletor não é responsável por saber se já baixou, coletor baixa tudo
    #O runner é quem controla o que baixar
    #Quando eu executo de novo o crawler interrompido, o coletor vai começar do 0 e gerar os chunks. O runner vai ver o que baixar
    
    #Com buscas muito grandes, isso pode demorar alguns minutos no retry para gerar os chunks de novo, mesmo sem baixar
    #O snapshot consolida tudo num arquivo só. É o conjunto de chunks num arquivo só.
    #Snapshot é em tese supérfluo, os chunks já lidam de não baixar tudo de novo, apenas acelera o processo
    #O runner quando cria o snapshot ele grava os chunks que eu baixei - é uma fotografia dos chunks que eu baixei
    #O snapshot por ser um arquivo único fica difícil de funcionar simultaneamente com vários workers, problemas de read

    #No RJ Snapshot ele é feito por ano


#No fim, importar task no CLI e no celery
#Para rodar o celery, usar o docker (rodar o docker compose - so´preciso mudar pra colocar a fila do carf na lista de crawlers)

    base.get_default_runner(
        collector=collector, 
        output=output, 
        handler=handler, 
        logger=logger, 
        max_workers=8) \
        .run(snapshot=snapshot)

@cli.command(name='carf')
@click.option('--rows'      , default=10,       help='Number of results per page')
@click.option('--sort'      , default='id asc', help='Sorting parameter')
@click.option('--ano-sessao', default=None,     help='Ano da sessão')
@click.option('--output-uri', default=None,     help='Output URI (e.g. gs://bucket_name')
@click.option('--enqueue'   , default=False,    help='Enqueue for a worker'  , is_flag=True)
@click.option('--split-tasks',
  default=None, help='Split tasks based on time range (weeks, months, days, etc) (use with --enqueue)')
def carf_command(rows, sort, ano_sessao, output_uri, enqueue, split_tasks):
    #args = (start_date, end_date, output_uri)
#   if enqueue:
#     if split_tasks:
#       start_date, end_date =\
#         pendulum.parse(start_date), pendulum.parse(end_date)
#       for start, end in utils.timely(start_date, end_date, unit=split_tasks, step=1):
#         task_id = carf_task.delay(
#           start.to_date_string(),
#           end.to_date_string(),
#           output_uri)
#         print(f"task {task_id} sent with params {start.to_date_string()} {end.to_date_string()}")
#     else:
#       carf_task.delay(*args)
#   else:
    carf_task(rows=rows,
            sort=sort,
            ano_sessao=ano_sessao,
            output_uri=output_uri
            )
