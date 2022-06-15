#!/usr/bin/env python3

# IMPORTS
#
from bs4 import BeautifulSoup
import utils
import requests
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException


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
import proxy

logger = logger_factory('tjmg')

# CONSTANTS
DEBUG = False
BASE_URL = 'https://www5.tjmg.jus.br/jurisprudencia'
TJMG_DATE_FORMAT = "DD/MM/YYYY"
STANDARD_DATE_FORMAT = "YYYY-MM-DD"
QUERY = 'NAO e'
DEFAULT_USER_AGENT = {
    # 'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:91.0) Gecko/20100101 Firefox/91.0')
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36 Edg/100.0.1185.44'
}
EXTRA_PARAMS = [
    # {'listaClasse': 600},
    # {'listaClasse': 602},
    {'excluirRepetitivos': 'true'}
]
RESULTS_PER_PAGE = 10  # 10, 20 or 50
IP = proxy.get_random_proxy()




def get_param_from_url(url, param):
    query = urlsplit(url).query
    value = dict(parse_qsl(query))[param]
    return value


def get_repetitivos(browser):
    soup = BeautifulSoup(browser.page_source())
    links = [element['href'] for element in soup.tbody.tbody.find_all('a')]
    return links


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
            'linhasPorPagina': RESULTS_PER_PAGE,
            'pesquisaPalavras': 'Pesquisar'}

def format_date(date):
    if not isinstance(date, str):
        return date.format(TJMG_DATE_FORMAT)
    else:
        return pendulum.from_format(date, STANDARD_DATE_FORMAT).format(TJMG_DATE_FORMAT)


def _get_search_url(session_id, start_date, end_date, query=None):
    query = query or default_filters()
    query['dataPublicacaoInicial'] = format_date(start_date)
    query['dataPublicacaoFinal'] = format_date(end_date)
    endpoint = f'{BASE_URL}/pesquisaPalavrasEspelhoAcordao.do'
    return f'{endpoint};jsessionid={session_id}?&{urlencode(query)}'


class TJMG(base.BaseCrawler, base.ICollector):

    def __init__(self, params, query, output, logger, handler, browser):
        super(TJMG, self).__init__(params, output, logger)
        self.query = query
        self.handler = handler
        self.browser = browser
        self.requester = requests.Session()

    def setup(self):
        try:
            url = f'{BASE_URL}/formEspelhoAcordao.do'
            browser = self.browser
            time.sleep(random.random())
            browser.get(url)
            self.session_id = browser.get_cookie('JSESSIONID')
            self.cookie_id = browser.get_cookie('juridico')
            self.headers = {
                'cookie': f'JSESSIONID={self.session_id};juridico={self.cookie_id}',
                **DEFAULT_USER_AGENT
            }

        except Exception as e:
            browser.quit()
            raise e

    @utils.retryable(max_retries=99)
    def count(self, init_date=None, query=None):
        url = f'{BASE_URL}/formEspelhoAcordao.do'
        start_date = init_date or self.params.get('start_date')
        end_date = init_date or self.params.get('end_date')
        query = query or default_filters()
        url = _get_search_url(
                    self.session_id,
                    query=query,
                    start_date=start_date,
                    end_date=end_date)
        self.logger.debug(f'GET {url}')
        response = self.requester.get(url, headers=self.headers, proxies = {'http':IP}, timeout=30, verify=False)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, features='html.parser')
            content = soup.find('p', class_='aviso')
            if content:
                number = ''.join(
                    [char for char in content.text if char.isdigit()])
                if number:
                    return int(number)
                else:
                    return 0
            else:
                results = soup.find('p', class_='info').contents[0]
                number = int(
                    ''.join(char for char in results if char.isdigit()))
                return number

        elif response.status_code == 401:
            CaptchaSolver(date=start_date,
                        logger=self.logger,
                        browser=self.browser,
                        requester=self.requester,
                        headers=self.headers,
                        session_id=self.session_id,
                        query=query).solve_captcha()
            if init_date:
                return self.count(init_date)
            else:
                return self.count()
        else:
            raise Exception(
                f'Unexpected status code {response.status_code} fetching {url}.')

    def chunks(self):
        self.total_records = self.count()
        self.total_pages = math.ceil(self.total_records/RESULTS_PER_PAGE)

        dates = [start for start, end in utils.timely(
            start_date=self.params.get('start_date'),
            end_date=self.params.get('end_date'),
            unit='days',
            step=1)]

        queries = [{**default_filters(), **extra} for extra in EXTRA_PARAMS]
        for date in dates:
            for query in queries:
                query_total_records = self.count(date, query)
                query_total_pages = math.ceil(query_total_records/RESULTS_PER_PAGE)

                for page in range(1, query_total_pages + 1):
                    keys = {
                        'date': date.format(STANDARD_DATE_FORMAT),
                        'page': page
                    }

                    if query.get('listaClasse'):
                        keys.update({'listaClasse': query.get('listaClasse')})
                    elif query.get('excluirRepetitivos'):
                        keys.update(
                            {'excluirRepetitivos': query.get('excluirRepetitivos')})

                    yield TJMGChunk(
                        keys=keys,
                        prefix='',
                        page=page,
                        headers=self.headers,
                        logger=self.logger,
                        browser=self.browser,
                        session_id=self.session_id,
                        requester=self.requester,
                        output=self.output,
                        query=query
                    )


class CaptchaSolver(TJMG):
    def __init__(self, date, logger, browser, requester, headers, session_id, query):
        self.logger = logger
        self.browser = browser
        self.date = date
        self.requester = requester
        self.headers = headers
        self.session_id = session_id
        self.query = query

    @utils.retryable(max_retries=33, sleeptime=20)
    def solve_captcha(self):
        browser = self.browser
        self.query['excluirRepetitivos'] = True
        url = _get_search_url(
                self.session_id,
                start_date=self.date,
                end_date=self.date,
                query=self.query
                )
        self.logger.debug(f'(Captcha) GET {url}')
        browser.get(url)
        while not browser.is_text_present('Resultado da busca') \
                and not browser.is_text_present('Nenhum Espelho do Acórdão foi encontrado'):
            browser.wait_for_element(locator=(By.ID, 'captcha_text'))
            response = self.requester.get(f'{BASE_URL}/captchaAudio.svl', headers=self.headers, proxies = {'http': IP}, timeout=30, verify=False)
            text = self._recognize_audio_by_content(response.content)
            captcha_box = browser.driver.find_element_by_id('captcha_text')
            captcha_box.send_keys(text)
            # for char in text:
            #     captcha_box.send_keys(char)
                # time.sleep(random.random())
            # time.sleep(1+random.random())
            if browser.is_text_present('não corresponde', tag='div'):
                browser.click(self._find(id='gerar'))
            else:
                try:
                    browser.wait_for_element(
                        locator=(By.CLASS_NAME, 'caixa_processo'),
                        timeout=20)
                except TimeoutException:
                    if browser.is_text_present('Acórdãos de Repetitivos:', tag='td'):
                        pass
                return

    @utils.retryable(max_retries=10, sleeptime=10)
    def _recognize_audio_by_content(self, content):
        filename = f'captcha_{"".join(choices(ascii_letters,k=10))}.wav'
        recognizer = sr.Recognizer()

        with open(filename, 'wb') as f:
            f.write(content)

        with sr.AudioFile(filename) as source:
            audio = recognizer.record(source)
            if not DEBUG:
                os.remove(filename)

        return recognizer.recognize_google(audio, language='pt-BR')


class TJMGChunk(base.Chunk):

    def __init__(self, keys, prefix, page, headers, logger, browser, session_id, requester, output, query):
        super(TJMGChunk, self).__init__(keys, prefix)
        self.page = page
        self.headers = headers
        self.logger = logger
        self.browser = browser
        self.session_id = session_id
        self.date = pendulum.parse(keys.get('date'))
        self.requester = requester
        self.output = output
        self.query = query

    @utils.retryable(max_retries=33, sleeptime=20)
    def rows(self):
        extra_query = {k: v
                        for k, v in self.query.items()
                        if k in ['ListaClasse', 'excluirRepetitivos']
                        }

        acts, _ = self.fetch_page(
                        self.date,
                        self.date,
                        self.page,
                        self.headers,
                        self.session_id,
                        extra_query)

        for act in acts:
            to_download = []
            query = self.query  # default_filters()
            query['paginaNumero'] = act
            query['numeroRegistro'] = act
            query['linhasPorPagina'] = 1
            url = _get_search_url(
                        self.session_id,
                        query=query,
                        start_date=self.date,
                        end_date=self.date)
            browser = self.browser
            browser.get(url)
            captcha = False
            while browser.is_text_present('Digite os números abaixo'):
                CaptchaSolver(
                    date=self.date,
                    logger=self.logger,
                    browser=self.browser,
                    requester=self.requester,
                    headers=self.headers,
                    session_id=self.session_id,
                    query=self.query).solve_captcha()
                captcha = True
            if captcha:
                browser.get(url)
            soup = BeautifulSoup(browser.page_source(), features="html5lib")
            error_message = soup.find('p',id='localizacao')
            if error_message and error_message.text == 'Sistema Indisponível':
                logger.warn(f'Error page for {url=}')
                continue

            date_label = soup.find('div', text='Data da publicação da súmula')

            proc_string = '_'.join([element.text for element in soup.find_all(
                'a', {'title': 'Abrir Andamento Processual'})])
            proc_string = ''.join(char for char in proc_string if char.isdigit() or char == '_')

            pub_date = date_label.find_next_sibling('div').text
            pub_date = pendulum.from_format(pub_date, TJMG_DATE_FORMAT)

            
            onclick_attr = soup.find('input', {"name": "inteiroTeorPDF"})
            if onclick_attr: 
                onclick_attr = onclick_attr['onclick']
                pdf_url = '='.join(onclick_attr.split('=')[1:]).strip("/'")
                pdf_url = f'{BASE_URL}/{pdf_url}'
                pdf_dest = f'{pub_date.year}/{pub_date.month:02d}/{pub_date.day:02d}_{proc_string}.pdf'
                to_download.append(base.ContentFromURL(
                                            src=pdf_url,
                                            dest=pdf_dest,
                                            content_type='application/pdf'))
            else:
                logger.warn(f'Inteiro not available for {proc_string}')

            html_dest = f'{pub_date.year}/{pub_date.month:02d}/{pub_date.day:02d}_{proc_string}.html'
            to_download.append(base.Content(
                                    content=browser.page_source(),
                                    dest=html_dest,
                                    content_type='text/html'))

            yield to_download

    @utils.retryable(max_retries=33, sleeptime=20)
    def fetch_page(self, start_date, end_date, page, headers, session_id, extra_query):
        query = default_filters()
        query['paginaNumero'] = page
        query.pop('listaClasse', None)
        query.pop('excluirRepetitivos', None)
        query.update(extra_query)
        url = _get_search_url(
            session_id,
            query=query,
            start_date=start_date,
            end_date=end_date
            )
        response = self.requester.get(url, headers=headers, proxies = {'http':IP}, timeout=30, verify=False)
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
                                next_page_link['href'],
                                param='paginaNumero')
            return (act_indexes, next_page)

        elif response.status_code == 401:
            CaptchaSolver(
                start_date,
                logger=self.logger,
                browser=self.browser,
                requester=self.requester,
                headers=self.headers,
                session_id=self.session_id,
                query=query
                ).solve_captcha()

            return self.fetch_page(
                            start_date,
                            end_date,
                            page,
                            headers,
                            session_id,
                            extra_query=extra_query
                            )
        else:
            raise Exception(
                f'Unexpected status code {response.status_code} fetching {url}.')

    def _find(self, matcher=None, **kwargs):
        return self._current_soup().find(matcher, **kwargs)

    def _current_soup(self):
        return utils.soup_by_content(self.browser.page_source())


@celery.task(queue='crawlers.tjmg', default_retry_delay=5 * 60,
             autoretry_for=(BaseException,),
             base=Singleton)
def tjmg_task(**kwargs):
    from logutils import logging_context

    with logging_context(crawler='tjmg'):
        output = utils.get_output_strategy_by_path(path=kwargs.get('output_uri'))
        logger.info(f'Output: {output}.')
        setup_cloud_logger(logger)

        params = {
            'start_date': pendulum.parse(kwargs.get('start_date')),
            'end_date': pendulum.parse(kwargs.get('end_date'))
        }

        query = default_filters()
        handler = base.ContentHandler(output=output)
        collector = TJMG(
            params=params,
            query=query,
            output=output,
            logger=logger,
            handler=handler,
            browser=browsers.FirefoxBrowser(headless=not DEBUG)
        )

        snapshot = base.Snapshot(keys=params)
        base.get_default_runner(
            collector=collector, output=output, handler=handler, logger=logger, max_workers=8) \
            .run(snapshot=snapshot)


@cli.command(name='tjmg')
@click.option('--start-date', prompt=True,   help='Format YYYY-mm-dd')
@click.option('--end-date', prompt=True,   help='Format YYYY-mm-dd')
@click.option('--output-uri', default=None,  help='Output URI (e.g. gs://bucket_name')
@click.option('--split-tasks',
                default='days', help='Split tasks based on time range (weeks, months, days, etc) (use with --enqueue)')
@click.option('--enqueue', default=False, help='Enqueue for a worker', is_flag=True)
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
                print(
                    f"task {task_id} sent with params {start.to_date_string()} {end.to_date_string()}")
        else:
            tjmg_task.delay(**kwargs)
    else:
        tjmg_task(**kwargs)
