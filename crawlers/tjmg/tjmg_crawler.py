#!/usr/bin/env python3

# IMPORTS
#
import requests
import utils
import json
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from selenium import webdriver
import time
import os
from urllib.parse import parse_qsl, urlencode, urlsplit
import speech_recognition as sr
from datetime import datetime, timedelta
import pendulum
import click
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm
from app import cli, celery
from logconfig import logger_factory
import browsers
import base

# CLI ARGUMENTS
#>>>>>>>>args_parser = utils.default_argument_parser()

# CONSTANTS
#
BASE_URL = 'https://www5.tjmg.jus.br/jurisprudencia'
DATE_FORMAT = "%d/%m/%Y" #utils.default_date_format()
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
    return str(date.strftime(DATE_FORMAT))

# SCRIPT
#


#class TJMG(utils.BSCrawler):
class TJMG(base.BaseCrawler):

    #self.browser = self.options['browser']
    # def __init__(self,**kwargs):
        
        
    #     #logger=logger, 
    #     #output=output, 
    #     #browser=driver,
    #     #requester=requests.Session(), 
    #     #query=query,
        
    #     #super().__init__(self,**kwargs)
    #     self.filters = kwargs['filters']
    #     self.browser = kwargs['browser']
    #     self.requester = kwargs['requester']

    def run(self):
        browser = self.options.get('browser')
        url = f'{BASE_URL}/formEspelhoAcordao.do'
        self.logger.info(f'GET {url}')
        browser.get(url)
        session_id = browser.get_cookie('JSESSIONID')
        cookie_id = browser.get_cookie('juridico')
        headers = {'cookie': f'JSESSIONID={session_id}; juridico={cookie_id}'}
        end_date = datetime.strptime(self.options.getparams['start_date'], DATE_FORMAT)
        date = datetime.strptime(self.params['end_date'], DATE_FORMAT)
        delta = timedelta(days=1)
        while date >= end_date:
            try:
                self.fetch_pages_by_date(date, headers, session_id)
                date -= delta
            except Exception as e:
                self.logger.fatal(f'{type(e).__name__}: "{e}"')
                self.browser.quit()
                raise e

        self.browser.quit()

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
        query = self.query.copy()
        query['paginaNumero'] = page
        url = self._get_search_url(
            session_id, query=query, date=format_date(date))
        self.logger.info(f'GET {url}')
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
            self.solve_captcha(date, headers, session_id)
            return self.fetch_page(date, page, headers, session_id)
        else:
            raise Exception(
                f'Unexpected status code {response.status_code} fetching {url}.')

    def fetch_act(self, date, act_index, headers, session_id):
        query = self.query.copy()
        query['paginaNumero'] = act_index
        query['numeroRegistro'] = act_index
        query['linhasPorPagina'] = 1
        url = self._get_search_url(
            session_id, query=query, date=format_date(date))
        self.logger.info(f'GET {url}')
        response = self.requester.get(url, headers=headers)
        if response.status_code == 200:
            soup = utils.soup_by_content(response.text)
            date_label = soup.find('div', text='Data da publicação da súmula')
            publication_date = date_label.find_next_sibling('div').text
            filepath = utils.get_filepath(publication_date, act_index, 'html')
            self.output.save_from_contents(
                filepath=filepath, contents=response.text)
        elif response.status_code == 401:
            self.solve_captcha(date, headers, session_id)
            return self.fetch_act(date, act_index, headers, session_id)
        else:
            raise Exception(
                f'Unexpected status code {response.status_code} fetching {url}.')

    @utils.retryable(max_retries=3, sleeptime=20, retryable_exceptions=(TimeoutException))
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
                    locator=(By.CLASS_NAME, 'caixa_processo'), timeout=15)
        return headers

    def _get_search_url(self, session_id, date, query=None):
        query = query or self.query.copy()
        query['dataPublicacaoInicial'] = query['dataPublicacaoFinal'] = date
        endpoint = f'{BASE_URL}/pesquisaPalavrasEspelhoAcordao.do'
        return f'{endpoint};jsessionid={session_id}?&{urlencode(query)}'

    def _recognize_audio_by_content(self, content):
        filename = 'captcha.wav'
        recognizer = sr.Recognizer()

        with open(filename, 'wb') as f:
            f.write(content)

        with sr.AudioFile(filename) as source:
            audio = recognizer.record(source)
            os.remove(filename)

        return recognizer.recognize_google(audio, language='pt-BR')


# if __name__ == "__main__":
#     args = args_parser.parse_args()
#     daterange = utils.get_daterange(args)

#     log_filename = f'logs/TJMG-{daterange}.log'
#     logger = utils.setup_logger(name=daterange, log_file=log_filename)

#     output = None

#     if args.bucket:
#         output = utils.GSOutput(bucket_name=args.bucket)
#         logger.info(f'Output gs://{args.bucket}.')
#     else:
#         output_folder = f'./out/TJMG/{daterange}'
#         output = utils.LSOutput(output_folder=output_folder)  # type: ignore
#         logger.info(f'Output file:///{output_folder}.')

#     headless = bool(args.headless)
#     firefox = utils.FirefoxBrowser(headless=headless)
#     query = default_filters()

#     crawler = TJMG(
#         logger=logger, output=output, browser=firefox,
#         requester=requests.Session(), query=query,
#     )

#     crawler.run()
#     logger.info(f'{args} execution ended. Files saved successfully.')



@celery.task(queue='crawlers', rate_limit='2/h', default_retry_delay=30 * 60,
             autoretry_for=(Exception,))
def tjmg_task(start_date, end_date, output_uri, pdf_async, skip_pdf):
    start_date, end_date =\
        pendulum.parse(start_date), pendulum.parse(end_date)

    output = utils.get_output_strategy_by_path(path=output_uri)
    logger = logger_factory('tjmg')
    logger.info(f'Output: {output}.')

    #headless = bool(args.headless)
    #firefox = browsers.FirefoxBrowser(headless=1)#headless)
    driver = webdriver.Chrome()
    driver.implicitly_wait(20)
    params = default_filters()
    filters = {'start_date':start_date,'end_date':end_date}

    crawler = TJMG(
        params=params,
        output=output,
        logger=logger, 
        options = dict(browser=driver,
        requester=requests.Session(), 
        filters=filters
        )
    )

    crawler.run()


@cli.command(name='tjmg')
@click.option('--start-date', prompt=True,   help='Format YYYY-MM-DD.')
@click.option('--end-date'  , prompt=True,   help='Format YYYY-MM-DD.')
@click.option('--output-uri', default=None,  help='Output URI (e.g. gs://bucket_name')
@click.option('--pdf-async' , default=False, help='Download PDFs async'   , is_flag=True)
@click.option('--skip-pdf'  , default=False, help='Skip PDF download'     , is_flag=True)
@click.option('--enqueue'   , default=False, help='Enqueue for a worker'  , is_flag=True)
def tjmg_command(start_date, end_date, output_uri, pdf_async, skip_pdf, enqueue):
  args = (start_date, end_date, output_uri, pdf_async, skip_pdf)
  if enqueue:
    tjmg_task.delay(*args)
  else:
    tjmg_task(*args)
