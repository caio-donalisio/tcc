import base
import celery
import click
import json
import math
import pendulum
import unidecode
import utils
import requests
import re

from app import cli, celery
from bs4 import BeautifulSoup
from logconfig import logger_factory, setup_cloud_logger

logger = logger_factory('tjdf')

BASE_URL = 'https://pesquisajuris.tjdft.jus.br/IndexadorAcordaos-web'
SOURCE_DATE_FORMAT='DD/MM/YYYY'
DEFAULT_HEADERS = {
    'authority': 'pesquisajuris.tjdft.jus.br',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'accept-language': 'en-US,en;q=0.9,pt;q=0.8',
    'cache-control': 'max-age=0',
    'origin': 'https://pesquisajuris.tjdft.jus.br',
    'referer': 'https://pesquisajuris.tjdft.jus.br/IndexadorAcordaos-web/sistj?visaoId=tjdf.sistj.acordaoeletronico.buscaindexada.apresentacao.VisaoBuscaAcordao',
    'upgrade-insecure-requests': '1',
    'user-agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
                        ' AppleWebKit/537.36 (KHTML, like Gecko)'
                        ' Chrome/92.0.4515.131 Safari/537.36 Edg/92.0.902.67'),

}

RESULTS_PER_PAGE = 20

def get_filters(start_date, end_date, **kwargs):
    return {
      'visaoId': 'tjdf.sistj.acordaoeletronico.buscaindexada.apresentacao.VisaoBuscaAcordao',
      'controladorId': 'tjdf.sistj.acordaoeletronico.buscaindexada.apresentacao.ControladorBuscaAcordao',
      'idDoUsuarioDaSessao': '',
      'nomeDaPagina': 'selecaoTipoResultado',
      'comando': 'selecionarBase',
      'enderecoDoServlet': 'sistj',
      'visaoAnterior': 'tjdf.sistj.acordaoeletronico.buscaindexada.apresentacao.VisaoBuscaAcordao',
      'skin': '',
      'historicoDePaginas': 'buscaLivre',
      'argumentoDePesquisa': '',
      'vgtrli': '',
      'numero': '',
      'tipoDeNumero': 'NumAcordao',
      'lcuhra': '',
      'desembargador': '',
      'ltrvln': '',
      'tipoDeRelator': 'TODOS',
      'orgaoJulgador': '',
      'descricaoDaClasse': '',
      'kixncs': '',
      'ementa': '',
      'decisaoLivre': '',
      'indexacao': '',
      'idDaClasse': '',
      'codigoFeito2Instancia': '',
      'tipoDeBusca': '',
      'camposSelecionados': [
          'ESPELHO',
          'INTEIROTEOR',
      ],
      'camposDeAgrupamento': '',
      'numeroDaPaginaAtual': '11',
      'quantidadeDeRegistros': str(RESULTS_PER_PAGE),
      'dataInicio': f'{start_date}',
      'dataFim': f'{end_date}',
      'campoDeOrdenacao': '',
      'baseDados': [
          'BASE_ACORDAOS_IDR',
          'BASE_TEMAS',
          'BASE_ACORDAOS',
          'BASE_INFORMATIVOS',
      ],
      'baseSelecionada': 'BASE_ACORDAO_TODAS',
      'mostrarPaginaSelecaoTipoResultado': 'true',
      'totalHits': '0',
      'tipoDeData': 'DataJulgamento',
      'jurisprudenciaAdministrativa': '',
      'faixaInicial': '',
      'faixaFinal': '',
      'indexados': '',
      'decisao': '',
      'decisaolivre': '',
      'loginDoUsuario': '',
      'nomeDoUsuario': '',
      'idClasse': '',
      'descricaoClasse': '',
      'idDoAcordao': '',
      'rlfodv': 'aberto',
      'idJanelaAbrirAjax': '',
      'idJanelaFecharAjax': '',
      'idJanelaAbrirIsModalAjax': 'false',
      'internet': '1',
      'colunaParaOrdenar': '',
      'fieldSetParaOrdenar': 'resultado',
      'comandoFieldSet_resultado': '',
      'selecaoFieldSet_resultado': '',
      'selecaoFieldSet_resultado_FIELD_SET_FILHO': '',
      'linhaEmEdicaoFieldSet_resultado': '',
      'linhaEmExclusaoFieldSet_resultado': '',
      'resultado': 'descricao,base,valor',
  }


class TJDFClient:

    def __init__(self):
        self.session = requests.Session()
        self.url = f'{BASE_URL}/sistj'

    @utils.retryable(max_retries=3)
    def count(self, filters):
        result = self.fetch(filters)
        pattern = re.compile(".*Total de Documentos Encontrados:\s*(\d+).*", re.MULTILINE | re.DOTALL)
        counter = re.search(pattern, result.text)
        if counter:
          count = int(counter.group(1))
        else:
          count = 0
        return count

    @utils.retryable(max_retries=3)
    def fetch(self, filters, page=1):
        self.session.headers.update(DEFAULT_HEADERS)
        try:
            return self.session.post(
                url=f'{self.url}',
                data={
                    **get_filters(**filters),
                    'numeroDaPaginaAtual': str(page),
                    'quantidadeDeRegistros': str(RESULTS_PER_PAGE)
                }
            )

        except Exception as e:
            logger.error(f"page fetch error params: {filters}")
            raise e


class TJDFCollector(base.ICollector):

    def __init__(self, client, filters):
        self.client  = client
        self.filters = filters

    def count(self, period=None):
        if self.filters.get('count_only'):
            return self.client.count_periods(self.filters)
        elif period:
            return self.client.count(period)
        else:
            return self.client.count(self.filters)

    def chunks(self):
        total = self.count()
        pages = math.ceil(total/RESULTS_PER_PAGE)

        for page in range(1, pages + 1):
            yield TJDFChunk(
                keys={
                    **self.filters  , **{'page': page}
                },
                prefix='',
                filters=self.filters,
                page=page,
                client=self.client,
            )


class TJDFChunk(base.Chunk):

    def __init__(self, keys, prefix, filters, page, client):
        super(TJDFChunk, self).__init__(keys, prefix)
        self.filters = filters
        self.page = page
        self.client = client


    def rows(self):
        result = self.client.fetch(self.filters, self.page)
        JUDGMENTS_SELECTOR = "#tabelaResultado > thead ~ tr"
        soup = BeautifulSoup(result.text, 'html.parser')
        for judgment_row in soup.select(JUDGMENTS_SELECTOR):
            cols = judgment_row.find_all('td')
            record = {
              'numero_documento': cols[1].text,
              'data_julgamento': cols[4].text,
              'data_publicacao': cols[5].text,
              'orgao_julgador': cols[6].text,
              **self._extract_numero_processo(cols[3]),
              **self._get_judgment_document_metadata(cols[1].text)
            }

            session_at = pendulum.from_format(record['data_julgamento'], SOURCE_DATE_FORMAT)
            base_path   = f'{session_at.year}/{session_at.month:02d}'

            codigo = re.sub("\:", "-", record['numero_documento'])
            numero = record['numero_processo']

            dest_record = f"{base_path}/doc_{numero}_{codigo}.json"
            dest_report = f"{base_path}/doc_{numero}_{codigo}.pdf"
            report_url = self._get_judgment_document_url(record['numero_documento'])

            if report_url is None:
                logger.warn(f"Not found 'Inteiro Teor' for judgment {record['numero_documento']}")
                yield [
                    base.Content(content=json.dumps(record),dest=dest_record,
                        content_type='application/json'),
                ]
            else:
              yield [
                  base.Content(content=json.dumps(record),dest=dest_record,
                      content_type='application/json'),
                  base.ContentFromURL(src=report_url,dest=dest_report,
                      content_type='application/pdf')
              ]



    def _extract_numero_processo(self, tag):
      for child in tag.find_all('b'):
        pattern = '^Processo\:\s*(.*)$'
        match = re.search(pattern, child.text)
        if match:
          return {
            "numero_processo": match.group(1)
          }


    def _get_judgment_document_metadata(self, judgment_id):
      data = {
        **get_filters(**self.filters),
        'numeroDoDocumento': f"{judgment_id}",
        'comando': 'abrirDadosDoAcordao'
      }
      response = requests.post(f'{BASE_URL}/sistj', headers=DEFAULT_HEADERS, data=data)
      soup = BeautifulSoup(response.text, 'html.parser')
      fields_mappper = {
        'classe_do_processo': 'classe_processo',
        'registro_do_acordao_numero': 'numero_documento',
        'data_de_julgamento': 'data_julgamento',
        'orgao_julgador': 'orgao_julgador',
        'relator': 'relator',
        'relator_designado': 'relator_designado',
        'ementa': 'ementa',
        'decisa': 'decisao',
        'jurisprudencia_em_temas': 'jurisprudencia_em_temas',
        'exibir_com_formatacao\n\nexibir_sem_formatacao': 'texto_sem_formatacao'
      }
      metadata = {}
      for row in soup.find_all('div', {'class': 'linha'}):
        label = row.find('div', {'class': 'textoRotulo'})
        value = row.find('div', {'class': 'conteudoComRotulo'})
        if label and value:
          field = self._normalize(label.text.strip())
          if field in fields_mappper:
            metadata[f'{fields_mappper.get(field)}'] = value.text.strip()

      return metadata


    def _normalize(self, label):
      return unidecode.unidecode(label.lower().replace(':', '').replace(' ', '_'))


    def _get_judgment_document_url(self, judgment_id):
      data = {
        **get_filters(**self.filters),
        'idDoAcordao': f"pje{judgment_id}",
        'comando': 'downloadInteiroTeorPaginaPesquisa'
      }
      response = requests.post(f'{BASE_URL}/sistj', headers=DEFAULT_HEADERS, data=data)
      soup = BeautifulSoup(response.text, 'html.parser')
      pattern = re.compile('iframeDownload\.setAttribute\("src","(infra\/Download\.jsp\?idd=.*)"\);', re.MULTILINE)
      script = soup.find("script", text=pattern)
      if script:
        match = pattern.search(script.string)
        if match:
          return f'{BASE_URL}/{match.group(1)}'

      return None

@celery.task(queue='crawlers.tjdf', default_retry_delay=5 * 60,
            autoretry_for=(BaseException,))
def tjdf_task(**kwargs):
    import utils
    setup_cloud_logger(logger)

    from logutils import logging_context

    with logging_context(crawler='tjdf'):
        output = utils.get_output_strategy_by_path(path=kwargs.get('output_uri'))
        logger.info(f'Output: {output}.')

        start_date = pendulum.parse(kwargs.get('start_date')).format(SOURCE_DATE_FORMAT)
        end_date = pendulum.parse(kwargs.get('end_date')).format(SOURCE_DATE_FORMAT)

        filters = {
            'start_date' :start_date,
            'end_date': end_date,
        }

        collector = TJDFCollector(
            client=TJDFClient(),
            filters=filters
        )
        handler   = base.ContentHandler(output=output)
        snapshot = base.Snapshot(keys=filters)

        base.get_default_runner(
            collector=collector,
            output=output,
            handler=handler,
            logger=logger,
            max_workers=8) \
            .run(snapshot=snapshot)


@cli.command(name='tjdf')
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
def tjdf_command(**kwargs):
  if kwargs.get('enqueue'):
    if kwargs.get('split_tasks'):
      start_date = pendulum.parse(kwargs.get('start_date'))
      end_date = pendulum.parse(kwargs.get('end_date'))
      for start, end in utils.timely(start_date, end_date, unit=kwargs.get('split_tasks'), step=1):
        task_id = tjdf_task.delay(
          start_date=start.to_date_string(),
          end_date=end.to_date_string(),
          output_uri=kwargs.get('output_uri'))
        print(f"task {task_id} sent with params {start.to_date_string()} {end.to_date_string()}")
    else:
      tjdf_task.delay(**kwargs)
  else:
    tjdf_task(**kwargs)
