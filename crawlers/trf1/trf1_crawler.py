from socket import timeout
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
import re

SITE_KEY = '6LfkZ24UAAAAAMO1KEF_pP-G3wE0dYN69-SG8NxI' # k value of recaptcha, found inside page
WEBSITE_URL = 'https://www2.cjf.jus.br/jurisprudencia/trf1/index.xhtml'
TRF1_DATE_FORMAT = 'DD/MM/YYYY'
CRAWLER_DATE_FORMAT = 'YYYY-MM-DD'
DATE_PATTERN = re.compile(r'\d{2}/\d{2}/\d{4}')
FILES_PER_PAGE = 50  #10, 30 or 50

logger = logger_factory('trf1')

class TRF1Client:

    def __init__(self):
        import browsers
        self.browser = browsers.FirefoxBrowser(headless=True)

    @utils.retryable(max_retries=9, sleeptime=20)
    def setup(self):
        self.browser.get(WEBSITE_URL)

    @property
    def page_searched(self):
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(self.browser.page_source(), 'html.parser')
        return bool(soup.find(name='span', attrs={'class':"ui-paginator-current"}))

    @utils.retryable(max_retries=9, sleeptime=20)
    def make_search(self,filters):
        import captcha
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.support.ui import WebDriverWait
        self.browser.wait_for_element((By.ID, "formulario:ckbAvancada")).click()
        
        #SELECT DATA DE PUBLICAÇÃO
        WebDriverWait(self.browser.driver, 10).until(EC.element_to_be_clickable((By.CLASS_NAME,'ui-icon-triangle-1-s'))).click() 
        WebDriverWait(self.browser.driver, 10).until(EC.element_to_be_clickable((By.ID,'formulario:combo_tipo_data_1'))).click()
        
        #INSERT DATE RANGE
        WebDriverWait(self.browser.driver, 10).until(EC.element_to_be_clickable((By.ID, "formulario:j_idt37_input"))).send_keys(pendulum.parse(filters.get('start_date')).format(TRF1_DATE_FORMAT))
        WebDriverWait(self.browser.driver, 10).until(EC.element_to_be_clickable((By.ID, "formulario:j_idt39_input"))).send_keys(pendulum.parse(filters.get('end_date')).format(TRF1_DATE_FORMAT))
       
        captcha.solve_recaptcha(self.browser, logger, SITE_KEY)
        
        #CLICK 'PESQUISAR'
        self.browser.driver.find_element_by_id('formulario:actPesquisar').click()
        self.browser.driver.implicitly_wait(10)

        WebDriverWait(self.browser.driver, 10).until(EC.element_to_be_clickable((By.ID, 'formulario:tabelaDocumentos:j_id23')))
        self.browser.select_by_id(field_id='formulario:tabelaDocumentos:j_id23', option=FILES_PER_PAGE)
        
        self.browser.driver.implicitly_wait(10)

    @utils.retryable(max_retries=9, sleeptime=20)
    def count(self, filters):
        result = self.fetch(filters)
        # soup = BeautifulSoup(result, features='html.parser')
        div = result.find(id='formulario:j_idt61:0:j_idt65:0:ajax')
        count = int(''.join(char for char in div.text if char.isdigit()))
        return count

    @utils.retryable(max_retries=9, sleeptime=20)
    def fetch(self, filters, page=1):
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.common.by import By
        import bs4
        
        while not self.page_searched:
            self.setup()
            self.make_search(filters)
        
        soup = bs4.BeautifulSoup(self.browser.page_source(), 'html.parser')
        PAGE_PATTERN = r'.*Página: (\d+)\/.*'
        current_page = int(re.search(PAGE_PATTERN, soup.find('span', class_='ui-paginator-current').text).group(1))
        # page = 2
        while  current_page != page:
            if not self.page_searched: 
                self.make_search(filters)
            current_page = int(re.search(PAGE_PATTERN, soup.find('span', class_='ui-paginator-current').text).group(1))
            soup = bs4.BeautifulSoup(self.browser.page_source(), 'html.parser')
            if current_page < page:
                to_click_class = 'ui-icon-seek-next'
            elif current_page > page:
                to_click_class = 'ui-icon-seek-prev'
            current_page = int(re.search(PAGE_PATTERN, soup.find('span', class_='ui-paginator-current').text).group(1))
            if current_page != page:
                WebDriverWait(self.browser.driver, 10).until(EC.element_to_be_clickable((By.CLASS_NAME, to_click_class))).click() 
            self.browser.driver.implicitly_wait(10)
            soup = bs4.BeautifulSoup(self.browser.page_source(), 'html.parser')
            current_page = int(re.search(PAGE_PATTERN, soup.find('span', class_='ui-paginator-current').text).group(1))
            
            
        return soup

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

        for page in range(1, pages + 1):
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
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.common.by import By
        
        DATE_PATTERN = r'Data da publicação[^\d]*(?P<date>(?P<day>\d{2})\/(?P<month>\d{2})\/(?P<year>\d{4}))'
        soup = self.client.fetch(self.filters, page=self.page)
        rows = soup.find_all(name='div', attrs={'class':"ui-datagrid-column ui-g-12 ui-md-12"})
        for row in rows:
            hash_str = utils.get_content_hash(row, [{'name':'td'}])
            titulo = row.find(attrs={'class':"titulo_doc"}).text
            process_number = ''.join(char for char in titulo if char.isdigit())
            title = re.sub(r'[^\d\-\.]+','',titulo)
            pub_date = re.search(DATE_PATTERN, row.text)
            dest_path = f"{pub_date.groupdict()['year']}/{pub_date.groupdict()['month']}/{pub_date.groupdict().get('day')}_{process_number}_{hash_str[:10]}.html"
            to_download = []
            to_download.append(base.Content(content=str(row),dest=dest_path,
                content_type='text/html'))

            link = row.find('a',text='Acesse aqui').get('href')
            if 'ConsultaPublica/listView.seam' in link:
                import browsers
                
                from selenium import webdriver
                from selenium.webdriver.common.by import By
                from selenium.webdriver.common.keys import Keys
                from selenium.webdriver.support.wait import WebDriverWait
                from selenium.webdriver.support import expected_conditions as EC
                import requests, time



                # options = webdriver.FirefoxOptions()
                # options.set_preference("browser.download.folderList", 2)
                # options.set_preference("browser.download.dir", f"/home/caiod/Inspira/inspira-crawlers/data/trf1/{pub_date.groupdict().get('year')}/{pub_date.groupdict().get('month')}/")
                # options.set_preference("browser.download.useDownloadDir", True)
                # options.set_preference("browser.helperApps.neverAsk.saveToDisk", "application/pdf")
                # options.set_preference("pdfjs.disabled", True)
                # driver = webdriver.Firefox(options = options, executable_path="geckodriver")
                browser = browsers.FirefoxBrowser(headless=False, pub_date=pub_date)

                # driver.get("https://www.okcc.online/")
                # driver.maximize_window()

                browser.get(link)
                browser.fill_in('fPP:numProcesso-inputNumeroProcessoDecoration:numProcesso-inputNumeroProcesso', title)
                browser.driver.implicitly_wait(10)
                WebDriverWait(browser.driver, 10).until(EC.element_to_be_clickable((By.ID,'fPP:searchProcessos'))).click() 
                browser.driver.implicitly_wait(10)
                inteiro_soup = BeautifulSoup(browser.page_source(), 'html.parser')
                count = inteiro_soup.find('span',attrs={'class':"text-muted"}, text=re.compile(r'.*resultados encontrados'))
                count = ''.join(char for char in count.text if char.isdigit()) or 0
                count = int(count)
                LINK_PATTERN = r'\n*Visualizar documentos(?P<date>\d{2}\/\d{2}\/\d{4}) (?P<time>\d{2}:\d{2}:\d{2}) - (?P<doc>[\s\w]+)(?P<doc_2> \([\s\w]+\)?)'
                # if count == 1:
                # browser.driver.implicitly_wait(10)
                import time
                # WebDriverWait(browser.driver, 10).until(EC.element_to_be_clickable((By.CLASS_NAME,'fa-external-link')))
                time.sleep(2)
                inteiro_soup = BeautifulSoup(browser.page_source(), 'html.parser')
                link = inteiro_soup.find('a', attrs={'title':'Ver Detalhes'})
                link = re.search(r".*\(\'Consulta pública\','(.*?)\'\)",link['onclick']).group(1)
                browser.get(f'https://pje2g.trf1.jus.br{link}')
                time.sleep(1)

                inteiro_soup = BeautifulSoup(browser.page_source(), 'html.parser') 
                table = inteiro_soup.find('table',attrs={'id':'j_id140:processoDocumentoGridTab'})
                available_links = table.find_all('a')
                available_links = [link for link in available_links if any(char.isdigit() for char in link.text)]
                available_links = [link for link in available_links if re.search(LINK_PATTERN, link.text).groupdict()['doc'] == 'Acórdão']
                available_links = [link for link in available_links if (pendulum.from_format(re.search(LINK_PATTERN, link.text).groupdict()['date'], TRF1_DATE_FORMAT) - pendulum.from_format(pub_date.groupdict()['date'], TRF1_DATE_FORMAT)).days < 14]
                print('LEN:', len(available_links))
                URL_PATTERN = r".*(http\:\/\/.*?)\'\)"
                new_link = re.search(URL_PATTERN, available_links[0]['onclick']).group(1)
                browser.get(new_link)
                # WebDriverWait(browser.driver, 10).until(EC.element_to_be_clickable((By.ID,available_links[0]['id']))).click() 
                # time.sleep(2)
                WebDriverWait(browser.driver, 10).until(EC.element_to_be_clickable((By.TAG_NAME,'i'))).click() 
                # base.Content
                # time.sleep(2)

                # new_soup = BeautifulSoup(browser.page_source(),'html.parser')
                # link_container =  new_soup.find('a',id='j_id47:downloadPDF')
                # DATA_PATTERN = r".*\'ca\'\:\'(?P<ca>.*)\',\'idProcDocBin\'\:\'(?P<idProcDocBin>\d+)\'.*"
                # session_data = re.search(DATA_PATTERN, link_container['onclick']).groupdict()

                # WebDriverWait(self.browser.driver, 10).until(EC.element_to_be_clickable((By.TAG_NAME,'i'))).click() 


                # post_data = {'j_id47': 'j_id47',
                # 'javax.faces.ViewState': 'j_id11',
                # 'j_id47:downloadPDF': 'j_id47:downloadPDF',
                # 'ca': session_data['ca'],
                # 'idProcDocBin':session_data['idProcDocBin']}

                # import requests
                # session = requests.Session()
                # session.get(new_link)
                # r = session.post(
                #     url='http://pje2g.trf1.jus.br/consultapublica/ConsultaPublica/DetalheProcessoConsultaPublica/documentoSemLoginHTML.seam',
                #     data=post_data
                # )

                # print(r)
                browser.driver.quit()
                

                # info = {}
                # for n,link in enumerate(available_links,1):
                #     info[n]['obj'] = link
                #     info[n]['date'] = re.search(LINK_PATTERN, link.text).groupdict().get('date')
                #     info[n]['doc'] = re.search(LINK_PATTERN, link.text).groupdict().get('doc')
                


                # def nearest_date(items, pivot):
                #     pivot = pendulum.from_format(pivot, TRF1_DATE_FORMAT)
                #     if items and pivot:
                #         return min([pendulum.from_format(item.text, TRF1_DATE_FORMAT) for item in items],
                #                 key=lambda x: abs(x - pivot))
                #     else:
                #         return ''


                print(4)

            elif 'PesquisaMenuArquivo' in link:
                import requests
                response = requests.get(link)

            
            
            yield to_download
            
        # for proc_number in range(
        #     1 + ((self.page - 1) * FILES_PER_PAGE),
        #     1 + min(self.total, ((self.page) * FILES_PER_PAGE))
        # ):
            

        #     response = self.client.session.get(
        #         f'https://web.trf1.jus.br/base-textual/Home/ListaColecao/9?np={proc_number}')#, headers=DEFAULT_HEADERS)
            
        #     if response.status_code != 200:
        #         logger.warn(f"Response <{response.status_code}> - {response.url}")
        #         raise utils.PleaseRetryException()

        #     soup = BeautifulSoup(response.text, features='html.parser')

        #     #THIS CHECK REQUIRES FURTHER TESTING
        #     if soup.find('h2', text='Para iniciar uma nova sessão clique em algum dos links ao lado.'):
        #         self.client.setup()
        #         logger.warn(f"Session expired - trying again")
        #         raise utils.PleaseRetryException()

        #     def file_is_error(soup):
        #         error_div = soup.find(name='div',id='erro')
        #         if error_div:
        #             is_error = error_div.find(text=re.compile(r'^[\s\n]*Ocorreu[\s\n]*um[\s\n]*erro\.?[\s\n]*$'))
        #         return bool(error_div and is_error)

           
        #     pub_date_div = soup.find('div', text='Data da Publicação/Fonte ')
        #     pub_date, = DATE_PATTERN.findall(
        #         pub_date_div.next_sibling.next_sibling.text)

        #     data_julg_div = soup.find('div', text='Data do Julgamento ')
        #     session_at = data_julg_div.next_sibling.next_sibling.text.strip()
        #     session_at = pendulum.from_format(session_at, TRF1_DATE_FORMAT)

        #     processo_text = soup.find(
        #         'h4', text='Processo').next_sibling.next_sibling.text.strip()
        #     processo_num = ''.join(
        #         char for char in processo_text if char.isdigit())

        #     content_hash = utils.get_content_hash(soup,
        #     tag_descriptions=[
        #         {'name':'p',    'class_': 'docTexto'},
        #         {'name':'div',  'class_': 'docTexto'},
        #         {'name':'pre',  'class_': 'txtQuebra'}
        #     ],
        #     length=40)

        #     dest_path = f'{session_at.year}/{session_at.month:02d}/{session_at.day:02d}_{processo_num}_{content_hash}.html'

        #     to_download.append(base.Content(
        #         content=BeautifulSoup(
        #             response.text, features='html5lib').encode('latin-1'),
        #         dest=dest_path,
        #         content_type='text/html'))

        #     url_page_acordao = soup.find(
        #         'a', {'title': 'Exibir a íntegra do acórdão.'}).get('href')
        #     page_acordao = requests.get(
        #         url_page_acordao)#, headers=DEFAULT_HEADERS, timeout=120)
        #     page_acordao_soup = BeautifulSoup(
        #         page_acordao.text, features='html5lib')

        #     link_date = nearest_date(page_acordao_soup.find_all(
        #         'a', text=re.compile('\d{2}/\d{2}/\d{4}')), pivot=pub_date)

        #     if link_date:
        #         link_to_inteiro = page_acordao_soup.find(
        #             'a', text=link_date.format(TRF1_DATE_FORMAT))
        #         dest_path_inteiro = f'{session_at.year}/{session_at.month:02d}/{session_at.day:02d}_{processo_num}_{content_hash}_INTEIRO.html'
        #         url_acordao_inteiro = link_to_inteiro.get('href')
        #         to_download.append(base.ContentFromURL(
        #             src=f'https://web.trf1.jus.br{url_acordao_inteiro}',
        #             dest=dest_path_inteiro,
        #             content_type='text/html'
        #         ))
        #     else:
        #         logger.error(
        #             f'Link not available for full document of: {processo_text}')

        #     yield to_download

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
        handler = base.ContentHandler(output=output)
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
