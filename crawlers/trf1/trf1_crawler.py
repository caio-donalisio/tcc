import base
import math
import pendulum
import celery
import utils
from logconfig import logger_factory, setup_cloud_logger
import click
from app import cli, celery
import requests
from bs4 import BeautifulSoup
import re
import hashlib
from itertools import chain
import re

DEFAULT_HEADERS = headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6,ja;q=0.5',
    'Cache-Control': 'max-age=0',
    'Connection': 'keep-alive',
    # Requests sorts cookies= alphabetically
    # 'Cookie': 'JSESSIONID=66BSdO0cBi46284GadjTQChC7oa6tdnuJNUuHh63.taturana04-hc02:jurisprudencia_node02; cookiesession1=678B29C73C8E711A6DC3B1056D62F9B7',
    'Origin': 'https://www2.cjf.jus.br',
    'Referer': 'https://www2.cjf.jus.br/jurisprudencia/trf1/index.xhtml',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.64 Safari/537.36 Edg/101.0.1210.47',
    'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="101", "Microsoft Edge";v="101"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
}
    #     "
    # 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    # 'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6,ja;q=0.5',
    # 'Cache-Control': 'max-age=0',
    # 'Connection': 'keep-alive',
    # # Requests sorts cookies= alphabetically
    # # 'Cookie': 'JSESSIONID=zr6gdnuSJZEerrbxeliMz3vA7m4SVeYePpS5UuxP.taturana04-hc02:jurisprudencia_node02; cookiesession1=678B29C73C8E711A6DC3B1056D62F9B7',
    # 'Origin': 'https://www2.cjf.jus.br',
    # 'Referer': 'https://www2.cjf.jus.br/jurisprudencia/trf1/',
    # 'Sec-Fetch-Dest': 'document',
    # 'Sec-Fetch-Mode': 'navigate',
    # 'Sec-Fetch-Site': 'same-origin',
    # 'Sec-Fetch-User': '?1',
    # 'Upgrade-Insecure-Requests': '1',
    # 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.64 Safari/537.36 Edg/101.0.1210.47',
    # 'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="101", "Microsoft Edge";v="101"',
    # 'sec-ch-ua-mobile': '?0',
    # 'sec-ch-ua-platform': '"Windows"',


TRF1_DATE_FORMAT = 'DD/MM/YYYY'
CRAWLER_DATE_FORMAT = 'YYYY-MM-DD'
DATE_PATTERN = re.compile(r'\d{2}/\d{2}/\d{4}')
FILES_PER_PAGE = 30

logger = logger_factory('trf1')


@utils.retryable(max_retries=9, sleeptime=20)
def get_captcha_response():
    for n in range(1,11):
        headers= {
            'User-Agent':'PostmanRuntime/7.29.0',
            'Accept':'*/*',
            'Accept-Encoding':'gzip, deflate, br',
            'Connection':'keep-alive'
        }
        data = {
            'action':'upload',
            'key':'INSERIR KEY AQUI',
            'captchatype':3,
            'gen_task_id':int(pendulum.now().format('x')),
            'sitekey':'6LfkZ24UAAAAAMO1KEF_pP-G3wE0dYN69-SG8NxI',
            'pageurl':'https://www2.cjf.jus.br/jurisprudencia/trf1/index.xhtml',
        }
        logger.info(f'Fetching ReCaptcha response...')
        response = requests.post('http://fasttypers.org/Imagepost.ashx', data=data, headers=headers)#, verify=False)
        if response.status_code == 200 and len(response.text) > 50:
            logger.info(f'ReCaptcha Success: {response.text[:10]}...')
            return response.text
        else:
            logger.info(f'ReCaptcha Failure: Attempt #{n}...')
            raise utils.PleaseRetryException

def get_post_data(filters):
    return {
        "formulario": "formulario",
        "formulario:textoLivre": "",
        "formulario:ckbAvancada_input": "on",
        "formulario:j_idt17": "",
        "formulario:j_idt19": "",
        "formulario:j_idt21": "",
        "formulario:j_idt23": "",
        "formulario:j_idt25": "",
        "formulario:j_idt27": "",
        "formulario:j_idt29": "",
        "formulario:j_idt31": "",
        "formulario:j_idt33": "",
        "formulario:j_idt35": "",
        "formulario:j_idt37_input": "01/05/2020",#.pendulum.parse(filters.get('start_date')).format(TRF1_DATE_FORMAT),
        "formulario:j_idt39_input": "31/05/2020",#pendulum.parse(filters.get('end_date')).format(TRF1_DATE_FORMAT),
        "formulario:combo_tipo_data_focus": "",
        "formulario:combo_tipo_data_input": "DTPP",
        "formulario:selectTiposDocumento": "ACORDAO",
        "formulario:j_idt51": "TRF1",
        "formulario:actPesquisar": "",
        "g-recaptcha-response": get_captcha_response()
    }


class TRF1Client:

    def __init__(self):
        import browsers
        from requests import Session
        self.session = Session()
        #browsers.FirefoxBrowser(headless=True)
        #self.session = requests.Session()

    @utils.retryable(max_retries=9, sleeptime=20)
    def setup(self):
        from bs4 import BeautifulSoup
        self.session.headers.update(DEFAULT_HEADERS)

        response = self.session.get('https://www2.cjf.jus.br/jurisprudencia/trf1/index.xhtml')#, headers=DEFAULT_HEADERS)
        
        # javax_faces_ViewState = soup.find("input", {"type": "hidden", "name":"javax.faces.ViewState"})['value']

        
        self.cookies = response.cookies
        return BeautifulSoup(response.text,'html.parser')
        # self.cookies['JSESSIONID'] = self.cookies
        # for cookie in response.cookies:
        # self.browser.get('https://www2.cjf.jus.br/jurisprudencia/trf1/index.xhtml')
        # self.session.get('https://www2.cjf.jus.br/jurisprudencia/trf1/index.xhtml',
                        #  headers=DEFAULT_HEADERS)

    @utils.retryable(max_retries=9, sleeptime=20)
    def count(self, filters):
        result = self.fetch(filters)
        
        soup = BeautifulSoup(result.text, features='html5lib')
        count = soup.find('span', {'class': 'class="ui-paginator-current"'})
        pattern = re.compile(r'Exibindo \d+ - \d+ de (\d+),.*')
        count = pattern.search(count.text).group(1)
        count = count.text if count else ''
        if count:
            return int(''.join([char for char in count if char.isdigit()]))
        else:
            return 0

    @utils.retryable(max_retries=9, sleeptime=20)
    def fetch(self, filters):
        javax = self.setup().find("input", {"type": "hidden", "name":"javax.faces.ViewState"})['value']
        post_data = {**get_post_data(filters),**{"javax.faces.ViewState":javax}}
        # post_data = get_post_data(filters)
        url = 'https://www2.cjf.jus.br/jurisprudencia/trf1/index.xhtml'
        
        from time import sleep
        sleep(4)
        page_response = self.session.post(
            url, 
            data=post_data,
            # cookies=self.cookies, 
            # headers=DEFAULT_HEADERS, 
            timeout=35,
            # verify=False,
        )
        print(5)


        # import time
        # from selenium.webdriver.common.by import By
        # from selenium.webdriver.support import expected_conditions as EC
        # from selenium.webdriver.support.ui import WebDriverWait


        # # time.sleep(1.324142)
        # # box = self.browser.driver.find_element(By.CLASS_NAME,'g-recaptcha')
        # # box = self.browser.driver.find_element(By.CLASS_NAME,'g-recaptcha')
        # # box = self.browser.driver.find_element(By.CLASS_NAME,'recaptcha-checkbox')
        
        # # WebDriverWait(self.browser.driver, 10).until(EC.element_to_be_clickable((By.ID, "formulario:ckbAvancada"))).click()
        # # adv_box = self.browser.driver.find_element_by_id()
        # # self.browser.click(adv_box)
        # from bs4 import BeautifulSoup
        # from selenium.webdriver.support.ui import Select
        # import random

        # #RECAPTCHA


        # # time.sleep(0.222515161 + random.random() * 1.234641 + random.random() * 0.434641)
        # # WebDriverWait(self.browser.driver, 10).until(EC.frame_to_be_available_and_switch_to_it((By.CSS_SELECTOR,"iframe[name^='a-'][src^='https://www.google.com/recaptcha/api2/anchor?']")))
        # # time.sleep(0.222515161 + random.random() * 1.234641 + random.random() * 0.434641)
        # # WebDriverWait(self.browser.driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//span[@id='recaptcha-anchor']"))).click()
        # # self.browser.driver.switch_to_default_content()

        # self.browser.driver.find_element_by_id('formulario:ckbAvancada').click()
        # WebDriverWait(self.browser.driver, 10).until(EC.element_to_be_clickable((By.ID, 'formulario:j_idt37_input')))
        # self.browser.fill_in('formulario:j_idt37_input',pendulum.parse(filters.get('start_date')).format(TRF1_DATE_FORMAT))
        # self.browser.fill_in('formulario:j_idt39_input',pendulum.parse(filters.get('end_date')).format(TRF1_DATE_FORMAT))
        
        # dropdown = self.browser.driver.find_element_by_id('formulario:combo_tipo_data_input')
        # self.browser.driver.execute_script("arguments[0].scrollIntoView();", dropdown)
        # # self.browser.driver.find_element_by_class_name('ui-icon-triangle-1-s').click() #ABRE MENU
        # WebDriverWait(self.browser.driver, 10).until(EC.element_to_be_clickable((By.ID,'ui-icon-triangle-1-s'))).click() 
        # WebDriverWait(self.browser.driver, 10).until(EC.element_to_be_clickable((By.ID,'formulario:combo_tipo_data_1'))).click() 
        # self.browser.driver.find_element_by_id('formulario:combo_tipo_data_1').click() #SELECIONA PUBLICAÇÃO

        
        # #RECAPTCHA (WIP)

        

        # dropdown = self.browser.driver.find_element_by_id('formulario:actPesquisar')
        # self.browser.driver.execute_script("arguments[0].scrollIntoView();", dropdown)
        # WebDriverWait(self.browser.driver, 10).until(EC.element_to_be_clickable((By.ID,'formulario:actPesquisar'))).click()

        # return self.browser.page_source


class TRF1Collector(base.ICollector):

    def __init__(self, client, filters):
        self.client = client
        self.filters = filters

    @utils.retryable(max_retries=9, sleeptime=20)
    def count(self):
        return self.client.count(self.filters)

    @utils.retryable(max_retries=9, sleeptime=20)
    def chunks(self):
        total = self.count()
        pages = math.ceil(total/FILES_PER_PAGE)

        for page in range(1, pages + 2):
            yield TRF1Chunk(
                keys={**self.filters, **{'page': page}},
                prefix='',
                page=page,
                total=total,
                filters=self.filters,
                client=self.client,
            )


class TRF1Chunk(base.Chunk):

    def __init__(self, keys, prefix, page, total, filters, client):
        super(TRF1Chunk, self).__init__(keys, prefix)
        self.page = page
        self.total = total
        self.filters = filters
        self.client = client

    @utils.retryable(max_retries=9, sleeptime=20)
    def rows(self):
        self.client.fetch(self.filters)
        for proc_number in range(
            1 + ((self.page - 1) * FILES_PER_PAGE),
            1 + min(self.total, ((self.page) * FILES_PER_PAGE))
        ):
            to_download = []

            response = self.client.session.get(
                f'https://web.trf1.jus.br/base-textual/Home/ListaColecao/9?np={proc_number}', headers=DEFAULT_HEADERS)
            
            if response.status_code != 200:
                logger.warn(f"Response <{response.status_code}> - {response.url}")
                raise utils.PleaseRetryException()

            soup = BeautifulSoup(response.text, features='html.parser')

            #THIS CHECK REQUIRES FURTHER TESTING
            if soup.find('h2', text='Para iniciar uma nova sessão clique em algum dos links ao lado.'):
                self.client.setup()
                logger.warn(f"Session expired - trying again")
                raise utils.PleaseRetryException()

            def file_is_error(soup):
                error_div = soup.find(name='div',id='erro')
                if error_div:
                    is_error = error_div.find(text=re.compile(r'^[\s\n]*Ocorreu[\s\n]*um[\s\n]*erro\.?[\s\n]*$'))
                return bool(error_div and is_error)

           
            pub_date_div = soup.find('div', text='Data da Publicação/Fonte ')
            pub_date, = DATE_PATTERN.findall(
                pub_date_div.next_sibling.next_sibling.text)

            data_julg_div = soup.find('div', text='Data do Julgamento ')
            session_at = data_julg_div.next_sibling.next_sibling.text.strip()
            session_at = pendulum.from_format(session_at, TRF1_DATE_FORMAT)

            processo_text = soup.find(
                'h4', text='Processo').next_sibling.next_sibling.text.strip()
            processo_num = ''.join(
                char for char in processo_text if char.isdigit())

            content_hash = utils.get_content_hash(soup,
            tag_descriptions=[
                {'name':'p',    'class_': 'docTexto'},
                {'name':'div',  'class_': 'docTexto'},
                {'name':'pre',  'class_': 'txtQuebra'}
            ],
            length=40)

            dest_path = f'{session_at.year}/{session_at.month:02d}/{session_at.day:02d}_{processo_num}_{content_hash}.html'

            to_download.append(base.Content(
                content=BeautifulSoup(
                    response.text, features='html5lib').encode('latin-1'),
                dest=dest_path,
                content_type='text/html'))

            url_page_acordao = soup.find(
                'a', {'title': 'Exibir a íntegra do acórdão.'}).get('href')
            page_acordao = requests.get(
                url_page_acordao, headers=DEFAULT_HEADERS, timeout=120)
            page_acordao_soup = BeautifulSoup(
                page_acordao.text, features='html5lib')

            link_date = nearest_date(page_acordao_soup.find_all(
                'a', text=re.compile('\d{2}/\d{2}/\d{4}')), pivot=pub_date)

            if link_date:
                link_to_inteiro = page_acordao_soup.find(
                    'a', text=link_date.format(TRF1_DATE_FORMAT))
                dest_path_inteiro = f'{session_at.year}/{session_at.month:02d}/{session_at.day:02d}_{processo_num}_{content_hash}_INTEIRO.html'
                url_acordao_inteiro = link_to_inteiro.get('href')
                to_download.append(base.ContentFromURL(
                    src=f'https://web.trf1.jus.br{url_acordao_inteiro}',
                    dest=dest_path_inteiro,
                    content_type='text/html'
                ))
            else:
                logger.error(
                    f'Link not available for full document of: {processo_text}')

            yield to_download


class TRF1Handler(base.ContentHandler):
    def __init__(self, output, headers):
        super(TRF1Handler, self).__init__(output)
        self.headers = headers

    @utils.retryable(max_retries=9, sleeptime=20)
    def _handle_url_event(self, event):

        if self.output.exists(event.dest):
            return

        try:
            response = requests.get(event.src,
                                    allow_redirects=True,
                                    headers=self.headers,
                                    verify=False)

            dest = event.dest
            content_type = event.content_type

            if response.status_code == 200:
                self.output.save_from_contents(
                    filepath=dest,
                    contents=response.content,
                    content_type=content_type)
            else:
                logger.warn(
                    f"Response <{response.status_code}> - {response.url}")
        except requests.exceptions.Timeout as e:
            logger.error(f"Timeout Error: {e} - {event.src}")
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Connection Error: {e} - {event.src}")
        except requests.exceptions.RequestException as e:
            logger.error(f"General Request Error: {e} - {event.src}")


@celery.task(queue='crawlers.trf1', default_retry_delay=5 * 60,
             autoretry_for=(BaseException,))
def trf1_task(**kwargs):
    import utils
    setup_cloud_logger(logger)

    from logutils import logging_context

    with logging_context(crawler='trf1'):
        output = utils.get_output_strategy_by_path(
            path=kwargs.get('output_uri'))
        logger.info(f'Output: {output}.')

        query_params = {
            'start_date': kwargs.get('start_date'),
            'end_date': kwargs.get('end_date')
        }

        collector = TRF1Collector(client=TRF1Client(), filters=query_params)
        handler = TRF1Handler(output=output, headers=DEFAULT_HEADERS)
        snapshot = base.Snapshot(keys=query_params)

        base.get_default_runner(
            collector=collector,
            output=output,
            handler=handler,
            logger=logger,
            max_workers=8) \
            .run(snapshot=snapshot)


@cli.command(name='trf1')
@click.option('--start-date',    prompt=True,      help='Format YYYY-MM-DD.')
@click.option('--end-date',    prompt=True,      help='Format YYYY-MM-DD.')
@click.option('--output-uri',    default=None,     help='Output URI (e.g. gs://bucket_name')
@click.option('--enqueue',    default=False,    help='Enqueue for a worker', is_flag=True)
@click.option('--split-tasks',
              default=None, help='Split tasks based on time range (weeks, months, days, etc) (use with --enqueue)')
def trf1_command(**kwargs):
    if kwargs.get('enqueue'):
        if kwargs.get('split_tasks'):
            start_date = pendulum.parse(kwargs.get('start_date'))
            end_date = pendulum.parse(kwargs.get('end_date'))
            for start, end in utils.timely(start_date, end_date, unit=kwargs.get('split_tasks'), step=1):
                task_id = trf1_task.delay(
                    start_date=start.to_date_string(),
                    end_date=end.to_date_string(),
                    output_uri=kwargs.get('output_uri'))
                print(
                    f"task {task_id} sent with params {start.to_date_string()} {end.to_date_string()}")
        else:
            trf1_task.delay(**kwargs)
    else:
        trf1_task(**kwargs)
