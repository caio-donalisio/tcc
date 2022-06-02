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
            if True:#'ConsultaPublica/listView.seam' in link:
                import browsers
                from seleniumrequests import Firefox
                # from selenium import webdriver
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
                browser = browsers.FirefoxBrowser(headless=False)#, pub_date=pub_date)
                TRF1_SEARCH_LINK = 'https://pje2g.trf1.jus.br/consultapublica/ConsultaPublica/listView.seam'
                browser.get(TRF1_SEARCH_LINK)
                browser.fill_in('fPP:numProcesso-inputNumeroProcessoDecoration:numProcesso-inputNumeroProcesso', title)
                browser.driver.implicitly_wait(10)
                WebDriverWait(browser.driver, 10).until(EC.element_to_be_clickable((By.ID,'fPP:searchProcessos'))).click() 
                browser.driver.implicitly_wait(10)
                LINK_PATTERN = r'\n*(Visualizar documentos)?(?P<date>\d{2}\/\d{2}\/\d{4}) (?P<time>\d{2}:\d{2}:\d{2}) - (?P<doc>[\s\w]+)(?P<doc_2> \([\s\w]+\)?)'
                browser.driver.implicitly_wait(10)
                import time
                time.sleep(2)
                inteiro_soup = BeautifulSoup(browser.page_source(), 'html.parser')
                link = inteiro_soup.find('a', attrs={'title':'Ver Detalhes'})
                link = re.search(r".*\(\'Consulta pública\','(.*?)\'\)",link['onclick']).group(1)
                browser.get(f'https://pje2g.trf1.jus.br{link}')
                browser.driver.implicitly_wait(10)

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
                # WebDriverWait(browser.driver, 10).until(EC.element_to_be_clickable((By.TAG_NAME,'i'))).click() 
                # base.Content
                # time.sleep(2)

                new_soup = BeautifulSoup(browser.page_source(),'html.parser')
                link_container =  new_soup.find('a',id='j_id47:downloadPDF')
                DATA_PATTERN = r".*\'ca\'\:\'(?P<ca>.*)\',\'idProcDocBin\'\:\'(?P<idProcDocBin>\d+)\'.*"
                session_data = re.search(DATA_PATTERN, link_container['onclick']).groupdict()

                WebDriverWait(browser.driver, 10).until(EC.element_to_be_clickable((By.TAG_NAME,'i'))).click() 
                time.sleep(3)
                time.sleep(1)
                
                post_data = {'j_id47': 'j_id47',
                'javax.faces.ViewState': new_soup.find("input", {"type": "hidden", "name":"javax.faces.ViewState"})['value'],
                'j_id47:downloadPDF': 'j_id47:downloadPDF',
                'ca': session_data['ca'],
                'idProcDocBin':session_data['idProcDocBin']}

                import requests

                cookies = {
                    'JSESSIONID': browser.get_cookie('JSESSIONID')#R2hwndCXkAYiGuJNZnivYf9u-dN6FuhAnmuXhO7I.srvpje2gcons04',
                }

                data = {
                    'j_id47': 'j_id47',
                    'javax.faces.ViewState': new_soup.find("input", {"type": "hidden", "name":"javax.faces.ViewState"})['value'],
                    'j_id47:downloadPDF': 'j_id47:downloadPDF',
                    'ca': session_data['ca'],
                    'idProcDocBin': session_data['idProcDocBin'],
                }

                headers = {
                    'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:99.0) Gecko/20100101 Firefox/99.0',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.5',
                    # 'Accept-Encoding': 'gzip, deflate, br',
                    'Origin': 'https://pje2g.trf1.jus.br',
                    'Connection': 'keep-alive',
                    'Referer': f'https://pje2g.trf1.jus.br/consultapublica/ConsultaPublica/DetalheProcessoConsultaPublica/documentoSemLoginHTML.seam?ca={data["ca"]}&idProcessoDoc={data["idProcDocBin"]}',
                    'Upgrade-Insecure-Requests': '1',
                    'Sec-Fetch-Dest': 'document',
                    'Sec-Fetch-Mode': 'navigate',
                    'Sec-Fetch-Site': 'same-origin',
                    'Sec-Fetch-User': '?1',
                }



                response = requests.post('https://pje2g.trf1.jus.br/consultapublica/ConsultaPublica/DetalheProcessoConsultaPublica/documentoSemLoginHTML.seam', cookies=cookies, headers=headers, data=data)

                print(response.content)
                dest_path = f"{pub_date.groupdict()['year']}/{pub_date.groupdict()['month']}/{pub_date.groupdict().get('day')}_{process_number}_{hash_str[:10]}.pdf"

                to_download.append(base.Content(content=response.content, content_type='application/pdf',dest=dest_path))
                print(5)

                # response = browser.driver.request('POST', 'http://pje2g.trf1.jus.br/consultapublica/ConsultaPublica/DetalheProcessoConsultaPublica/documentoSemLoginHTML.seam', 
                # data=post_data, headers = headers)



                # import requests
                # session = requests.Session()
                # session.get(new_link)
                # r = session.post(
                #     url='',
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


                print('-')

            elif 'PesquisaMenuArquivo' in link:
                import requests
                response = requests.get(link)
                print('OUTRO')
            yield to_download
 
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
