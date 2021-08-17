#!/usr/bin/env python3

# IMPORTS
#
from bs4 import BeautifulSoup
import utils
import requests
from selenium.webdriver.common.by import By


import time
import os
from urllib.parse import parse_qsl, urlencode, urlsplit
import speech_recognition as sr
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
import random

logger = logger_factory('tjmg')

# CONSTANTS
BASE_URL = 'https://www5.tjmg.jus.br/jurisprudencia'
TJMG_DATE_FORMAT = "DD/MM/YYYY"
STANDARD_DATE_FORMAT = "YYYY-MM-DD"
QUERY = 'a$ ou b$'
DEFAULT_USER_AGENT = {
  'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:91.0) Gecko/20100101 Firefox/91.0')
}


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
    return pendulum.from_format(date,STANDARD_DATE_FORMAT).format(TJMG_DATE_FORMAT)


# class CaptchaSolver:
#     def init(self,browser,requester,logger,start_date,end_date,headers,session_id):
#         self.browser = browser
#         self.requester=requester
#         self.logger=logger
#         self.start_date=start_date
#         self.end_date=end_date
#         self.headers=headers
#         self.session_id=session_id

#     @utils.retryable(max_retries=33, sleeptime=20)
#     def solve_captcha(self, start_date,end_date, headers, session_id):
#         browser = self.browser
#         url = self._get_search_url(session_id, start_date=format_date(start_date),end_date=format_date(end_date))
#         self.logger.info(f'GET {url}')
#         browser.get(url)
#         while not browser.is_text_present('Resultado da busca'):
#             browser.wait_for_element(locator=(By.ID, 'captcha_text'))
#             response = self.requester.get(
#                 f'{BASE_URL}/captchaAudio.svl', headers=headers)
#             text = utils.recognize_audio_by_content(response.content)
#             browser.fill_in('#captcha_text', value=text)
#             time.sleep(0.5)
#             if browser.is_text_present('não corresponde', tag='div'):
#                 browser.click(self._find(id='gerar'))
#             else:
#                 self.browser.wait_for_element(
#                     locator=(By.CLASS_NAME, 'caixa_processo'), timeout=20)
#         return headers


#     def _get_search_url(self, session_id, start_date,end_date, query=None):
#         query = query or default_filters()# self.query.copy()
#         query['dataJulgamentoInicial'] = start_date.format(TJMG_DATE_FORMAT)
#         query['dataJulgamentoFinal'] = end_date.format(TJMG_DATE_FORMAT)
#         endpoint = f'{BASE_URL}/pesquisaPalavrasEspelhoAcordao.do'
#         return f'{endpoint};jsessionid={session_id}?&{urlencode(query)}'

#     def _recognize_audio_by_content(self, content):
#         filename = f'captcha_{"".join(choices(ascii_letters,k=10))}.wav'
#         recognizer = sr.Recognizer()

#         with open(filename, 'wb') as f:
#             f.write(content)

#         with sr.AudioFile(filename) as source:
#             audio = recognizer.record(source)
#             os.remove(filename)

#         return recognizer.recognize_google(audio, language='pt-BR')


class TJMG(base.BaseCrawler,base.ICollector):

    def __init__(self, params,query,output,logger,handler,browser,**kwargs):
        super(TJMG, self).__init__(params, output, logger)
        self.query = query
        self.handler = handler
        self.browser = browser
        self.requester = requests.Session()


    def setup(self):
        try:
            url = f'{BASE_URL}/formEspelhoAcordao.do'
            browser = self.browser
            browser.get(url)
            self.session_id  = browser.get_cookie('JSESSIONID')
            self.cookie_id = browser.get_cookie('juridico')
            self.headers = {
            'cookie': f'JSESSIONID={self.session_id};juridico={self.cookie_id}',
            **DEFAULT_USER_AGENT
            }

        except Exception as e:
            browser.quit()
            raise e
        
    @utils.retryable(max_retries=99)
    def count(self):
        url = f'{BASE_URL}/formEspelhoAcordao.do'
        date = self.params.get('start_date')
        query = default_filters()
        url = self._get_search_url(
            self.session_id, query=query, start_date=format_date(date),end_date=format_date(date))
        self.logger.info(f'GET {url}')
        response = self.requester.get(url, headers=self.headers)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text,features='html.parser')
            results = soup.find('p',class_='info').contents[0]
            number = int(''.join(char for char in results if char.isdigit()))
            return number
            
        elif response.status_code == 401:
            self.solve_captcha(date, self.headers, self.session_id)
            return self.count()
        else:
            raise Exception(
                f'Unexpected status code {response.status_code} fetching {url}.')
         
    def chunks(self):
        self.total_records = self.count()
        self.total_pages = math.ceil(self.total_records/10)

        for page in range(1, self.total_pages + 1):
            keys = {
                'date':self.params.get('start_date'),
                'page':page
            }

            yield TJMGChunk(
                keys=keys,
                prefix = self.params.get('start_date'),
                page=page,
                headers = self.headers,
                logger = self.logger,
                browser=self.browser,
                session_id = self.session_id,
                requester = self.requester,
                output=self.output
            )

            #self.setup()

    @utils.retryable(max_retries=33, sleeptime=20)
    def solve_captcha(self, date, headers, session_id):
        browser = self.browser
        url = self._get_search_url(session_id, start_date=format_date(date),end_date=format_date(date))
        self.logger.info(f'GET {url}')
        browser.get(url)
        while not browser.is_text_present('Resultado da busca'):
            browser.wait_for_element(locator=(By.ID, 'captcha_text'))
            response = self.requester.get(
                f'{BASE_URL}/captchaAudio.svl', headers=headers)
            text = utils.recognize_audio_by_content(response.content)
            browser.fill_in('#captcha_text', value=text)
            time.sleep(0.5)
            if browser.is_text_present('não corresponde', tag='div'):
                browser.click(self._find(id='gerar'))
            else:
                self.browser.wait_for_element(
                    locator=(By.CLASS_NAME, 'caixa_processo'), timeout=20)
        return headers


    def _get_search_url(self, session_id, start_date,end_date, query=None):
        query = query or default_filters()# self.query.copy()
        query['dataJulgamentoInicial'] = start_date.format(TJMG_DATE_FORMAT)
        query['dataJulgamentoFinal'] = end_date.format(TJMG_DATE_FORMAT)
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
        self.page = page
        self.headers = headers
        self.logger = logger
        self.browser = browser
        self.session_id = session_id
        self.start_date = pendulum.from_format(keys.get('start_date'),STANDARD_DATE_FORMAT)
        self.date = pendulum.from_format(keys.get('end_date'),STANDARD_DATE_FORMAT)
        self.requester = requester
        self.output = output
        
    def rows(self):
        acts,next_page = self.fetch_page(self.start_date,self.end_date,self.page,self.headers,self.session_id)
        for act in acts:
            query=default_filters()
            query['paginaNumero'] = act
            query['numeroRegistro'] = act
            query['linhasPorPagina'] = 1
            url = self._get_search_url(
                self.session_id, query=query, start_date=self.start_date,end_date=self.end_date)
            self.logger.info(f'GET {url}')
            browser = self.browser
            browser.get(url)
            time.sleep(random.random())
            captcha = False
            while browser.is_text_present('Digite os números abaixo'):
                self.solve_captcha(self.date,self.headers,self.session_id)
                captcha = True
            if captcha:
                    browser.get(url)
            time.sleep(random.random())

            browser.wait_for_element(locator=(By.ID, 'imgBotao1'))
            browser.click(self._find(id='imgBotao1'))
            
            
            soup = BeautifulSoup(browser.page_source(),features="html5lib")
            date_label = soup.find('div', text='Data de Julgamento')
            session_date = date_label.find_next_sibling('div').text
            session_date = pendulum.from_format(session_date,TJMG_DATE_FORMAT)

            onclick_attr = soup.find('input',{"name":"inteiroTeorPDF"})['onclick']
            pdf_url = '='.join(onclick_attr.split('=')[1:]).strip("/'")
            pdf_url = f'{BASE_URL}/{pdf_url}'

            pdf_dest = f'{session_date.year}/{session_date.month:02d}/{act}.pdf'
            html_dest = f'{session_date.year}/{session_date.month:02d}/{act}.html'
            
            yield[
                base.Content(content=browser.page_source(),
                    dest=html_dest,content_type='text/html'),
                base.ContentFromURL(src=pdf_url,dest=pdf_dest,
                    content_type='application/pdf')
            ]

    @utils.retryable(max_retries=33, sleeptime=20)
    def solve_captcha(self, date, headers, session_id):
        browser = self.browser
        url = self._get_search_url(session_id, start_date=format_date(date),end_date=format_date(date))
        self.logger.info(f'GET {url}')
        browser.get(url)
        while not browser.is_text_present('Resultado da busca'):
            browser.wait_for_element(locator=(By.ID, 'captcha_text'))
            response = self.requester.get(
                f'{BASE_URL}/captchaAudio.svl', headers=headers)
            text = utils.recognize_audio_by_content(response.content)
            browser.fill_in('#captcha_text', value=text)
            time.sleep(0.5)
            if browser.is_text_present('não corresponde', tag='div'):
                browser.click(self._find(id='gerar'))
            else:
                self.browser.wait_for_element(
                    locator=(By.CLASS_NAME, 'caixa_processo'), timeout=20)
        return headers

    @utils.retryable(max_retries=33, sleeptime=20)#, retryable_exceptions=(TimeoutException))
    def fetch_page(self, start_date,end_date, page, headers, session_id):
        query = default_filters()# self.query.copy()
        query['paginaNumero'] = page
        url = self._get_search_url(
            session_id, query=query, start_date=start_date,end_date=end_date)
        self.logger.info(f'GET {url}')
        #self.requester.verify = False
        time.sleep(6)
        #response = requests.get(url,allow_redirects=False,verify=False)#,headers=headers,verify=False)
        response = self.requester.get(url, headers=headers)
        next_page = None
        if response.status_code == 200:
            soup = utils.soup_by_content(response.text)
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
            self.solve_captcha(start_date,end_date, headers, session_id)
            return self.fetch_page(start_date,end_date, page, headers, session_id)
        else:
            raise Exception(
                f'Unexpected status code {response.status_code} fetching {url}.')


    def _find(self, matcher=None, **kwargs):
        return self._current_soup().find(matcher, **kwargs)

    def _current_soup(self):
        return utils.soup_by_content(self.browser.page_source())

    def _get_search_url(self, session_id, start_date,end_date, query=None):
        query = query or default_filters()# self.query.copy()
        query['dataJulgamentoInicial'] = start_date.format(TJMG_DATE_FORMAT)
        query['dataJulgamentoFinal'] = end_date.format(TJMG_DATE_FORMAT)
        endpoint = f'{BASE_URL}/pesquisaPalavrasEspelhoAcordao.do'
        return f'{endpoint};jsessionid={session_id}?&{urlencode(query)}'

    


@celery.task(queue='crawlers.tjmg', default_retry_delay=5 * 60,
             autoretry_for=(BaseException,),
             base=Singleton)
def tjmg_task(**kwargs):
  from logutils import logging_context

  with logging_context(crawler='tjmg'):
    output = utils.get_output_strategy_by_path(path=kwargs.get('output_uri'))
    logger.info(f'Output: {output}.')
    setup_cloud_logger(logger)

    #options = dict(pdf_async=pdf_async, skip_pdf=skip_pdf)
    params = {
      'start_date': kwargs.get('start_date'), 'end_date': kwargs.get('end_date')
    }

    query = default_filters()
    handler = base.ContentHandler(output=output) # TJRJHandler(output=output)
    #options_browser = Options()
    #options_browser.page_load_strategy = 'eager'
    collector = TJMG(
                params=params,
                query = query,
                output=output,
                logger=logger,
                handler = handler,
                browser=browsers.FirefoxBrowser(),
                )

    snapshot = base.Snapshot(keys=params)
    base.get_default_runner(
        collector=collector, output=output, handler=handler, logger=logger, max_workers=8) \
      .run(snapshot=snapshot)



@cli.command(name='tjmg')
@click.option('--start-date', prompt=True,   help='Format YYYY-mm-dd')
@click.option('--end-date'  , prompt=True,   help='Format YYYY-mm-dd')
@click.option('--output-uri', default=None,  help='Output URI (e.g. gs://bucket_name')
@click.option('--split-tasks',
  default='days', help='Split tasks based on time range (weeks, months, days, etc) (use with --enqueue)')
@click.option('--enqueue'   , default=False, help='Enqueue for a worker'  , is_flag=True)
def tjmg_command(**kwargs):
  if kwargs.get('enqueue'):
    if kwargs.get('split_tasks'):
      start_date = pendulum.parse(kwargs.get('start_date'))
      end_date = pendulum.parse(kwargs.get('end_date'))
      for start, end in utils.timely(start_date, end_date, unit=kwargs.get('split_tasks'), step=1):
        task_id = tjmg_task.delay(
          start_date=start.to_date_string(),
          end_date=end.to_date_string(),
          output_uri=kwargs.get('output_uri'))
        print(f"task {task_id} sent with params {start.to_date_string()} {end.to_date_string()}")
    else:
      tjmg_task.delay(**kwargs)
  else:
    tjmg_task(**kwargs)
