#!/usr/bin/env python3

# IMPORTS
#
from bs4 import BeautifulSoup
import utils
import requests
#from . import tjmg_utils
from crawlers.tjmg import tjmg_utils
import json
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException
import time
import os
from urllib.parse import parse_qsl, urlencode, urlsplit
import speech_recognition as sr
from datetime import datetime, timedelta
from logconfig import logger_factory, setup_cloud_logger
from string import ascii_letters
from random import choices
from app import cli, celery
import click
import base
from celery_singleton import Singleton
import browsers
import math
import pendulum
import re

logger = logger_factory('tjmg')

# CLI ARGUMENTS
args_parser = tjmg_utils.default_argument_parser()

# CONSTANTS
#
BASE_URL = 'https://www5.tjmg.jus.br/jurisprudencia'
DATE_FORMAT = tjmg_utils.default_date_format()
QUERY = 'a$ ou b$'


# FUNCTIONS
#
def get_param_from_url(url, param):
    query = urlsplit(url).query
    value = dict(parse_qsl(query))[param]
    return value


def default_filters():
    return {'palavras': QUERY,
            'dataPublicacaoInicial': '',
            'dataPublicacaoFinal': '',
            'numeroRegistro': '1',
            'pesquisarPor': 'ementa',
            'orderByData': '2',
            'codigoOrgaoJulgador': '',
            'codigoCompostoRelator': '',
            'classe': '',
            'codigoAssunto': '',
            'dataJulgamentoInicial': '',
            'dataJulgamentoFinal': '',
             #'excluirRepetitivos':'true',
            'siglaLegislativa': '',
            'referenciaLegislativa': 'Clique+na+lupa+para+pesquisar+as+refer%EAncias+cadastradas...',
            'numeroRefLegislativa': '',
            'anoRefLegislativa': '',
            'legislacao': '',
            'norma': '',
            'descNorma': '',
            'complemento_1': '',
            'listaPesquisa': '',
            'descricaoTextosLegais': '',
            'observacoes': '',
            'linhasPorPagina': '10',
            'pesquisaPalavras': 'Pesquisar'}


def format_date(date):
    if isinstance(date,str):
        return date
    else:
        return str(date.strftime(DATE_FORMAT))

# SCRIPT
#


class TJMG(base.BaseCrawler,base.ICollector):#(tjmg_utils.BSCrawler):

    def __init__(self, params,query,output,logger,handler,browser,**kwargs):
        self.params = params
        self.query = query
        self.output = output
        self.logger = logger
        self.handler = handler
        self.browser = browser
        self.requester = requests.Session()

    

#     def run(self):
#         browser = self.browser
#         url = f'{BASE_URL}/formEspelhoAcordao.do'
#         self.logger.info(f'GET {url}')
#         browser.get(url)
#         session_id = browser.get_cookie('JSESSIONID')
#         cookie_id = browser.get_cookie('juridico')
#         headers = {'cookie': f'JSESSIONID={session_id}; juridico={cookie_id}'}
#         end_date = datetime.strptime(self.params.get('start_date'), DATE_FORMAT)
#         date = datetime.strptime(self.params.get('end_date'), DATE_FORMAT)
# #        end_date = datetime.strptime(args.start_date, DATE_FORMAT)
# #        date = datetime.strptime(args.end_date, DATE_FORMAT)
#         delta = timedelta(days=1)
#         while date >= end_date:
#             try:
#                 self.fetch_pages_by_date(date, headers, session_id)
#                 date -= delta
#             except Exception as e:
#                 self.logger.fatal(f'{type(e).__name__}: "{e}"')
#                 self.browser.quit()
#                 raise e

#         self.browser.quit()

    def setup(self):
        try:
            url = f'{BASE_URL}/formEspelhoAcordao.do'
            browser = self.browser
            browser.get(url)
            self.session_id  = browser.get_cookie('JSESSIONID')
            self.cookie_id = browser.get_cookie('juridico')
            self.headers = {
            'cookie': f'JSESSIONID={self.session_id};juridico={self.cookie_id}'
            }

        #finally:
        except Exception as e:
            browser.quit()
            raise e

        
    @utils.retryable(max_retries=99)
    def count(self,page=1):
#        browser = self.browser
        #self.setup()
        url = f'{BASE_URL}/formEspelhoAcordao.do'
        self.logger.info(f'GET {url}')
#        browser.get(url)
        #session_id = browser.get_cookie('JSESSIONID')
        #cookie_id = browser.get_cookie('juridico')
        #headers = {'cookie': f'JSESSIONID={session_id}; juridico={cookie_id}'}
        date = datetime.strptime(self.params.get('start_date'), DATE_FORMAT)
        query = default_filters()# self.query.copy()
        query['paginaNumero'] = page
        url = self._get_search_url(
            self.session_id, query=query, date=format_date(date))
        self.logger.info(f'GET {url}')
        response = self.requester.get(url, headers=self.headers)
        next_page = None
        if response.status_code == 200:
            soup = BeautifulSoup(response.text,features='html.parser')
            results = soup.find('p',class_='info').contents[0]
            number = int(''.join(char for char in results if char.isdigit()))
            return number
            
        elif response.status_code == 401:
            return 200
            self.solve_captcha(date, self.headers, self.session_id)
            return self.count()
        else:
            raise Exception(
                f'Unexpected status code {response.status_code} fetching {url}.')
         
    def chunks(self):
        self.total_records = 200
        #self.total_records = self.count()
        self.total_pages = math.ceil(self.total_records/10)

        for page in range(1, self.total_pages + 1):
            keys = {
                'date':self.params.get('start_date'),
                #'end_date':self.params.get('end_date'),
                'page':page
            }

            yield TJMGChunk(
                keys=keys,
                prefix = 'date_',
                page=page,
                headers = self.headers,
                logger = self.logger,
                browser=self.browser,
                session_id = self.session_id,
                requester = self.requester,
                output=self.output
            )

            # page = 1
            # while True:
            #     act_indexes, next_page = self.fetch_page(
            #         date, page, headers, session_id)
            #     for act_index in act_indexes:
            #         self.fetch_act(date, act_index, headers, session_id)

            #     if next_page is not None:
            #         page = int(next_page)
            #     else:
            #         break

    @tjmg_utils.retryable(max_retries=3, sleeptime=20, retryable_exceptions=(TimeoutException))
    def solve_captcha(self, date, headers, session_id):
        browser = self.browser
        url = self._get_search_url(session_id, date=format_date(date))
        self.logger.info(f'GET {url}')
        browser.get(url)
        while not browser.is_text_present('Resultado da busca'):
            browser.wait_for_element(locator=(By.ID, 'captcha_text'))
            response = self.requester.get(
                f'{BASE_URL}/captchaAudio.svl', headers=headers)
            text = self._recognize_audio_by_content(response.content)
            browser.fill_in('#captcha_text', value=text)
            time.sleep(0.5)
            if browser.is_text_present('não corresponde', tag='div'):
                browser.click(self._find(id='gerar'))
            else:
                self.browser.wait_for_element(
                    locator=(By.CLASS_NAME, 'caixa_processo'), timeout=20)
        return headers

    def _get_search_url(self, session_id, date, query=None):
        query = query or default_filters()# self.query.copy()
        query['dataPublicacaoInicial'] = query['dataPublicacaoFinal'] = date
        endpoint = f'{BASE_URL}/pesquisaPalavrasEspelhoAcordao.do'
        return f'{endpoint};jsessionid={session_id}?&{urlencode(query)}'

    def _recognize_audio_by_content(self, content):
        filename = f'captcha_{"".join(choices(ascii_letters,k=10))}.wav'
        recognizer = sr.Recognizer()

        with open(filename, 'wb') as f:
            f.write(content)

        with sr.AudioFile(filename) as source:
            audio = recognizer.record(source)
            os.remove(filename)

        return recognizer.recognize_google(audio, language='pt-BR')

    def teardown(self):
         return


class TJMGChunk(base.Chunk):

    def __init__(self, keys, prefix,page, headers, logger,browser,session_id,requester,output):
        super(TJMGChunk, self).__init__(keys, prefix)
        #self.keys_ = keys
        self.page = page
        self.headers = headers
        self.logger = logger
        self.browser = browser
        self.session_id = session_id
        self.date = keys.get('date')
        self.requester = requester
        self.output = output
        
    @utils.retryable(max_retries=99)
    def rows(self):

        delimiter_pattern = re.compile(r'[\.\-]')
        process_number_pattern = re.compile(r'(\S+)')
        process_fields = ['numero','digito','ano','orgao','tribunal','origem']

        acts,next_page = self.fetch_page(self.date,self.page,self.headers,self.session_id)
        for act in acts:
            #content = self.fetch_act(self.date,act,self.headers,self.session_id)
            query=default_filters()
            query['paginaNumero'] = act
            query['numeroRegistro'] = act
            query['linhasPorPagina'] = 1
            url = self._get_search_url(
                self.session_id, query=query, date=format_date(self.date))
            self.logger.info(f'GET {url}')
            browser = self.browser
            browser.get(url)
            while not browser.is_text_present('Inteiro Teor'):
                self.solve_captcha(self.date,self.headers,self.session_id)
            browser.click(self._find(id='imgBotao1'))
            
            
            soup = BeautifulSoup(browser.page_source(),features="html5lib")
            date_label = soup.find('div', text='Data de Julgamento')
            session_date = date_label.find_next_sibling('div').text
            session_date = pendulum.from_format(session_date,'DD/MM/YYYY')

            # process_number, = soup.find_all('a',
            #     attrs={'title' : 'Abrir Andamento Processual' })[-1].contents
            # process_number = process_number_pattern.search(process_number).group()
            # process_number = re.split(delimiter_pattern,process_number)
            
            # process_code = {k:v for k,v in zip(process_fields,process_number)}
            
            # num = str(int(process_code['numero'])//10)
            # ano = process_code['ano'][2:]
            # origem = process_code['origem'][-3:]

            # pdf_url = (
            #     f'https://www5.tjmg.jus.br/jurisprudencia/'
            #     f'relatorioEspelhoAcordao.do?inteiroTeor=true&ano={ano}'
            #     f'&ttriCodigo=1&codigoOrigem={origem}&numero={num}'
            #     f'&sequencial=1&sequencialAcordao=0'
            #     )
            
            onclick_attr = soup.find('input',{"name":"inteiroTeorPDF"})['onclick']
            pdf_url = '='.join(onclick_attr.split('=')[1:]).strip("/'")

            pdf_url = f'{BASE_URL}/{pdf_url}'
            pdf_dest = f'{session_date.year}/{session_date.month}/{act}.pdf'
            html_dest = f'{session_date.year}/{session_date.month}/{act}.html'
            

            #filepath = tjmg_utils.get_filepath(publication_date, act, 'html')
            #date = pendulum.from_format(self.date,DATE_FORMAT)
            #filepath = 
            yield[
                base.Content(content=browser.page_source(),
                    dest=html_dest,content_type='text/html'),
                base.ContentFromURL(src=pdf_url,dest=pdf_dest,
                    content_type='application/pdf')
            ]
            #acts = self.fetch_pages_by_date(self.date,self.headers,self.session_id)
            


    def fetch_pages_by_date(self, date, headers, session_id):
        page = 1
        while True:
            act_indexes, next_page = self.fetch_page(
                date, page, headers, session_id)
            for act_index in act_indexes:
                self.fetch_act(date, act_index, headers, session_id)

            if next_page is not None:
                page = int(next_page)
            else:
                break

    def fetch_page(self, date, page, headers, session_id):
        query = default_filters()# self.query.copy()
        query['paginaNumero'] = page
        url = self._get_search_url(
            session_id, query=query, date=format_date(date))
        self.logger.info(f'GET {url}')
        response = self.requester.get(url, headers=headers)
        next_page = None
        if response.status_code == 200:
            soup = tjmg_utils.soup_by_content(response.text)
            act_indexes = []
            links = soup.find_all('a', class_='linkListaEspelhoAcordaos')
            for link in links:
                id = get_param_from_url(link['href'], param='numeroRegistro')
                act_indexes.append(id)
            next_page_link = soup.find('a', alt='pr?xima')
            if next_page_link is not None:
                next_page = get_param_from_url(
                    next_page_link['href'], param='paginaNumero')
            return (act_indexes, next_page)
        elif response.status_code == 401:
            self.solve_captcha(date, headers, session_id)
            return self.fetch_page(date, page, headers, session_id)
        else:
            raise Exception(
                f'Unexpected status code {response.status_code} fetching {url}.')

    def fetch_act(self, date, act_index, headers, session_id):
        query=default_filters()
        #query = self.query.copy()
        query['paginaNumero'] = act_index
        query['numeroRegistro'] = act_index
        query['linhasPorPagina'] = 1
        url = self._get_search_url(
            session_id, query=query, date=format_date(date))
        self.logger.info(f'GET {url}')
        browser = self.browser
        
     
        #browser.get(url)
        #print(browser.page_source())
        
        #hover = ActionChains(browser.driver).move_to_element(element_to_hover_over)
        #hover.perform()
        
        
        #response = self.requester.get(url, headers=headers)
        #if response.status_code == 200:
        browser.get(url)
        while not browser.is_text_present('Inteiro Teor'):
            self.solve_captcha(date,headers,session_id)
        browser.click(self._find(id='imgBotao1'))
#        element_to_hover_over = browser.get(url, wait_for=(By.XPATH, "//*[contains(text(),'Inteiro Teor')]"))
        #
        #imgBotao1
        #hover = ActionChains(browser.driver).move_to_element(element_to_hover_over)
        #hover.perform()

        soup = BeautifulSoup(browser.page_source())
        #soup = tjmg_utils.soup_by_content(response.text)
        date_label = soup.find('div', text='Data da publicação da súmula')
        publication_date = date_label.find_next_sibling('div').text
        filepath = tjmg_utils.get_filepath(publication_date, act_index, 'html')
        return browser.page_source()
        #self.output.save_from_contents(
        #    filepath=filepath, contents=browser.page_source())
        #elif response.status_code == 401:
        #     self.solve_captcha(date, headers, session_id)
        #     return self.fetch_act(date, act_index, headers, session_id)
        # else:
        #     raise Exception(
        #         f'Unexpected status code {response.status_code} fetching {url}.')

    @tjmg_utils.retryable(max_retries=3, sleeptime=20, retryable_exceptions=(TimeoutException))
    def solve_captcha(self, date, headers, session_id):
        browser = self.browser
        url = self._get_search_url(session_id, date=format_date(date))
        self.logger.info(f'GET {url}')
        browser.get(url)
        while not browser.is_text_present('Resultado da busca'):
            browser.wait_for_element(locator=(By.ID, 'captcha_text'))
            response = self.requester.get(
                f'{BASE_URL}/captchaAudio.svl', headers=headers)
            text = self._recognize_audio_by_content(response.content)
            browser.fill_in('#captcha_text', value=text)
            time.sleep(0.5)
            if browser.is_text_present('não corresponde', tag='div'):
                browser.click(self._find(id='gerar'))
            else:
                self.browser.wait_for_element(
                    locator=(By.CLASS_NAME, 'caixa_processo'), timeout=20)
        return headers

    def _find(self, matcher=None, **kwargs):
        return self._current_soup().find(matcher, **kwargs)

    def _current_soup(self):
        return self.soup_by_content(self.browser.page_source())

    def soup_by_content(self,content):
        return BeautifulSoup(content, features='html.parser')

    def _get_search_url(self, session_id, date, query=None):
        query = query or default_filters()# self.query.copy()
        query['dataPublicacaoInicial'] = query['dataPublicacaoFinal'] = date
        endpoint = f'{BASE_URL}/pesquisaPalavrasEspelhoAcordao.do'
        return f'{endpoint};jsessionid={session_id}?&{urlencode(query)}'

    def _recognize_audio_by_content(self, content):
        filename = f'captcha_{"".join(choices(ascii_letters,k=10))}.wav'
        recognizer = sr.Recognizer()

        with open(filename, 'wb') as f:
            f.write(content)

        with sr.AudioFile(filename) as source:
            audio = recognizer.record(source)
            os.remove(filename)

        return recognizer.recognize_google(audio, language='pt-BR')


# if __name__ == "__main__":
#     args = args_parser.parse_args()
#     daterange = tjmg_utils.get_daterange(args)

#     log_filename = f'logs/TJMG-{daterange}.log'
#     logger = tjmg_utils.setup_logger(name=daterange, log_file=log_filename)

#     output = None

#     if args.bucket:
#         output = tjmg_utils.GSOutput(bucket_name=args.bucket)
#         logger.info(f'Output gs://{args.bucket}.')
#     else:
#         output_folder = f'./out/TJMG/{daterange}'
#         output = tjmg_utils.LSOutput(output_folder=output_folder)  # type: ignore
#         logger.info(f'Output file:///{output_folder}.')

#     headless = bool(args.headless)
#     firefox = tjmg_utils.FirefoxBrowser(headless=True)
#     query = default_filters()

#     crawler = TJMG(
#         logger=logger, output=output, browser=firefox,
#         requester=requests.Session(), query=query,
#     )

#     crawler.run()
#     logger.info(f'{args} execution ended. Files saved successfully.')

@celery.task(queue='crawlers.tjmg', default_retry_delay=5 * 60,
             autoretry_for=(BaseException,),
             base=Singleton)
def tjmg_task(start_date, end_date, output_uri, pdf_async, skip_pdf):
  from logutils import logging_context

  with logging_context(crawler='tjmg'):
    output = utils.get_output_strategy_by_path(path=output_uri)
    logger.info(f'Output: {output}.')
    setup_cloud_logger(logger)

    options = dict(pdf_async=pdf_async, skip_pdf=skip_pdf)
    params = {
      'start_date': start_date, 'end_date': end_date
    }

    query = default_filters()
    handler = base.ContentHandler(output=output) # TJRJHandler(output=output)

    collector = TJMG(
                params=params,
                query = query,
                output=output,
                logger=logger,
                handler = handler,
                browser=browsers.FirefoxBrowser(),
                **options
                )

    snapshot = base.Snapshot(keys=params)
    base.get_default_runner(
        collector=collector, output=output, handler=handler, logger=logger, max_workers=8) \
      .run(snapshot=snapshot)



@cli.command(name='tjmg')
@click.option('--start-date', prompt=True,   help='Format dd/mm/YYYY.')
@click.option('--end-date'  , prompt=True,   help='Format dd/mm/YYYY.')
@click.option('--output-uri', default=None,  help='Output URI (e.g. gs://bucket_name')
@click.option('--pdf-async' , default=False, help='Download PDFs async'   , is_flag=True)
@click.option('--skip-pdf'  , default=False, help='Skip PDF download'     , is_flag=True)
@click.option('--enqueue'   , default=False, help='Enqueue for a worker'  , is_flag=True)
def tjmg_command(start_date, end_date, output_uri, pdf_async, skip_pdf, enqueue):
  args = (start_date, end_date, output_uri, pdf_async, skip_pdf)
  if enqueue:
    print("task_id", tjmg_task.delay(*args))
  else:
    tjmg_task(*args)