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

DEBUG = False
SITE_KEY = '6LfkZ24UAAAAAMO1KEF_pP-G3wE0dYN69-SG8NxI' # k value of recaptcha, found inside page
WEBSITE_URL = 'https://www2.cjf.jus.br/jurisprudencia/trf1/index.xhtml'
TRF1_DATE_FORMAT = 'DD/MM/YYYY'
CRAWLER_DATE_FORMAT = 'YYYY-MM-DD'
DATE_PATTERN = re.compile(r'\d{2}/\d{2}/\d{4}')
FILES_PER_PAGE = 30  #10, 30 or 50
PDF_URL = 'https://pje2g.trf1.jus.br/consultapublica/ConsultaPublica/DetalheProcessoConsultaPublica/documentoSemLoginHTML.seam'
TRF1_SEARCH_LINK = 'https://pje2g.trf1.jus.br/consultapublica/ConsultaPublica/listView.seam'
DOC_TO_PDF_CONTAINER_URL = 'http://localhost/unoconv/pdf'

logger = logger_factory('trf1')

class TRF1Client:

    def __init__(self):
        import browsers
        self.browser = browsers.FirefoxBrowser(headless=not DEBUG)

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

        #SELECT NUMBER OF PROCESS PER PAGE
        WebDriverWait(self.browser.driver, 10).until(EC.element_to_be_clickable((By.ID, 'formulario:tabelaDocumentos:j_id23')))
        self.browser.select_by_id(field_id='formulario:tabelaDocumentos:j_id23', option=FILES_PER_PAGE)
        
        self.browser.driver.implicitly_wait(10)

    @utils.retryable(max_retries=9, sleeptime=20)
    def count(self, filters):
        result = self.fetch(filters)
        div = result.find(id='formulario:j_idt61:0:j_idt65:0:ajax')
        count = int(''.join(char for char in div.text if char.isdigit()))
        return count

    @utils.retryable(max_retries=9, sleeptime=20)
    def fetch(self, filters, page=1):
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.common.by import By

        def get_current_page(browser):
            return int(re.search(
                PAGE_PATTERN, self.browser.bsoup().find('span', class_='ui-paginator-current').text).group(1)
                )

        while not self.page_searched:
            self.setup()
            self.make_search(filters)
        
        PAGE_PATTERN = r'.*Página: (\d+)\/.*'
        
        rows = True     
        while get_current_page(self.browser) != page:
            self.browser.driver.implicitly_wait(20)
            if not self.page_searched or not rows: 
                self.make_search(filters)
            self.browser.driver.implicitly_wait(20)
            current_page = get_current_page(self.browser)
            if current_page != page:
                to_click_class = 'ui-icon-seek-next' if current_page < page else 'ui-icon-seek-prev'
                WebDriverWait(self.browser.driver, 20).until(EC.element_to_be_clickable((By.CLASS_NAME, to_click_class))).click() 
            self.browser.driver.implicitly_wait(20)
            rows = self.browser.bsoup().find_all(name='div', attrs={'class':"ui-datagrid-column ui-g-12 ui-md-12"})
        return self.browser.bsoup()

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
        #REFACTOR 
        import browsers
        import hashlib

        def get_nearest_date(items, pivot):
                    pivot = pendulum.from_format(pivot, TRF1_DATE_FORMAT)
                    if items and pivot:
                        return min([pendulum.from_format(item, TRF1_DATE_FORMAT) for item in items],
                                key=lambda x: abs(x - pivot))
                    else:
                        return ''

        def click_next_document_page(browser, slider_id):
            try:
                slider_page_input = browser.driver.find_element_by_id(slider_id);
            except Exception as e:
                return
            browser.driver.execute_script("arguments[0].value =  Number(arguments[0].value) + 1;", slider_page_input);
            # slider_page = int(slider_page_input.get_attribute('value'));
            browser.driver.execute_script("A4J.AJAX.Submit('j_id141:j_id633',event,{'similarityGroupingId':'j_id141:j_id633:j_id635','actionUrl':'/consultapublica/ConsultaPublica/DetalheProcessoConsultaPublica/listView.seam','eventsQueue':'','containerId':'j_id141:j_id549','parameters':{'j_id141:j_id633:j_id635':'j_id141:j_id633:j_id635'},'status':'_viewRoot:status'} )");
            browser.driver.implicitly_wait(10)


        def collect_all_links(browser):
            links = []
            while True:
                soup = BeautifulSoup(browser.page_source(),'html.parser')
                table = soup.find(id=re.compile(r'j_id\d+:processoDocumentoGridTabPanel_body'))
                hypers = table.find_all('a')
                links.extend(hypers)
                last_page = table.find('td', class_='rich-inslider-right-num')
                if not last_page:
                    break
                last_page = int(last_page.text)
                current_page = table.find('input',attrs={'class':"rich-inslider-field-right rich-inslider-field"})
                current_page = int(current_page['value'])
                
                if last_page and not last_page < current_page + 1:
                    click_next_document_page(browser, 'j_id141:j_id633:j_id634Input')
                    browser.driver.implicitly_wait(20)
                else:
                    break
            
            return links

        def filter_links(links):
            links = [link for link in links if re.search(r'\d{2}',link.text)]
            links = [link for link in links if re.search('Acórdão', link.text, re.IGNORECASE + re.UNICODE)]
            links = list(set(links))
            return links

        DATE_PATTERN = r'Data da publicação[^\d]*(?P<date>(?P<day>\d{2})\/(?P<month>\d{2})\/(?P<year>\d{4}))'
        page_soup = self.client.fetch(self.filters, page=self.page)
        rows = page_soup.find_all(name='div', attrs={'class':"ui-datagrid-column ui-g-12 ui-md-12"})
        for n, row in enumerate(rows, 1):
            inteiro_page_content, pdf_content = '',''
            meta_hash = utils.get_content_hash(row, [{'name':'td'}])
            
            acordao_titulo = row.find(attrs={'class':"titulo_doc"}).text
            title = re.sub(r'[^\d\-\.]+','',acordao_titulo)
            process_number = ''.join(char for char in acordao_titulo if char.isdigit())
            
            pub_date = re.search(DATE_PATTERN, row.text)
            # base_path = f"{pub_date.groupdict()['year']}/{pub_date.groupdict()['month']}/{pub_date.groupdict().get('day')}_{self.page:02}_{n:02}_{process_number}_{hash_str}"
            
            to_download = []

            # to_download.append(base.Content(content=str(row), dest=f"{base_path}_A.html",
            #     content_type='text/html'))

            process_link = row.find('a',text='Acesse aqui').get('href')
            
            
            MAXIMUM_TIME_DISTANCE = 150
            try_pje = False
            if 'PesquisaMenuArquivo' in process_link:
                # continue
                

                import requests
                response = requests.get(process_link)
                soup = BeautifulSoup(response.text, 'html.parser')
                date_links = soup.find_all(href=re.compile(r'.*\.doc'),text=re.compile(r'\d{2}\/\d{2}\/\d{4}'))
                dates = [link.text for link in date_links]
                d = f"{pub_date['day']}/{pub_date['month']}/{pub_date['year']}"
                nearest_date = get_nearest_date(dates, d)
                
                if not (dates or nearest_date) or abs((pendulum.from_format(d,TRF1_DATE_FORMAT) - nearest_date).days) > MAXIMUM_TIME_DISTANCE:
                    logger.info(f"Trying to fetch {title} on PJE...")
                    try_pje=True
                
                else:
                # [link for link in available_links if (pendulum.from_format(re.search(LINK_PATTERN, link.text).groupdict()['date'], TRF1_DATE_FORMAT) - pendulum.from_format(pub_date.groupdict()['date'], TRF1_DATE_FORMAT)).days < 28]
                    date_links = [link for link in date_links if pendulum.from_format(link.text,TRF1_DATE_FORMAT) == nearest_date]
                    pdf_content = self.merge_pdfs_from_links(date_links, is_doc=True)
                    # to_download.append(base.Content(
                    #     content=self.merge_pdfs_from_links(date_links, is_doc=True), 
                    #     dest=f"{base_path}.pdf", content_type ='application/pdf')
                    # )
                
            if try_pje or 'ConsultaPublica/listView.seam' in process_link:

                
                # continue 
                browser = browsers.FirefoxBrowser(headless=not DEBUG)
                # LINK_PATTERN = r'\n*(Visualizar documentos)?(?P<date>\d{2}\/\d{2}\/\d{4}) (?P<time>\d{2}:\d{2}:\d{2}) - (?P<doc>[\s\w]+)(?P<doc_2> \([\s\w]+\)?)'

                success = self.search_trf1_process_documents(browser, title)
                inteiro_soup = browser.bsoup()

                error_div = inteiro_soup.find(text=re.compile(r'.*Unhandled or Wrapper.*'))
                if not success or error_div:
                    logger.warn(f'Document not available for: {title}')
                
                else:
                    inteiro_page_content = browser.page_source()
                    links = collect_all_links(browser)
                    ls = []
                    for link in filter_links(links):
                        D = r'.*(?P<date>\d{2}\/\d{2}\/\d{4}).*'
                        U = r".*\'(?P<pdf_link>http.*?)\'.*"
                        if re.search(U, link['onclick']) and re.search(D, link.text).group(1):
                            ls.append({
                                'date':re.search(D, link.text).group(1), 
                                'url': re.search(U, link['onclick']).group(1)
                                })
                    
                    nearest_date = get_nearest_date([l['date'] for l in ls], pub_date.groupdict().get('date'))
                    ls = [l for l in ls if l['date'] == nearest_date.format(TRF1_DATE_FORMAT)]
                    if not ls or abs(pendulum.from_format(pub_date.groupdict().get('date'), TRF1_DATE_FORMAT) - nearest_date).days > MAXIMUM_TIME_DISTANCE:
                        logger.info(f'Document not available for: {title}')
                        # continue
                    else:
                        browser.get(ls[0]['url'])
                        browser.driver.implicitly_wait(20)
                        pdf_content = self.download_pdf(browser)
                        # to_download.append(base.Content(
                        #     content=self.download_pdf(browser), content_type='application/pdf',
                        #     dest=f"{base_path}.pdf"))
                browser.driver.quit()

            HASH_LENGTH = 10
            pdf_hash = hashlib.sha1(pdf_content).hexdigest()[:HASH_LENGTH] if pdf_content else '0' * HASH_LENGTH
            base_path = f"{pub_date.groupdict()['year']}/{pub_date.groupdict()['month']}/{pub_date.groupdict().get('day')}_{process_number}_{meta_hash}_{pdf_hash}"

            to_download.append(
                base.Content(content=str(row), 
                dest=f"{base_path}_A.html",
                content_type='text/html'))
            
            if inteiro_page_content:
                to_download.append(
                     base.Content(content=inteiro_page_content, 
                     dest=f"{base_path}_B.html",
                    content_type='text/html')
                    )

            if pdf_content:
                to_download.append(
                    base.Content(content=pdf_content,  
                    dest=f"{base_path}.pdf", 
                    content_type ='application/pdf'))

            yield to_download

    @utils.retryable()
    def merge_pdfs_from_links(self, document_links, is_doc=False):
        import PyPDF2 
        from io import BytesIO
        TRF1_ARCHIVE = 'https://arquivo.trf1.jus.br'
        merger = PyPDF2.PdfFileMerger()
        for link in document_links:
            
            file = requests.get(f"{TRF1_ARCHIVE}{link['href']}")
            if file.status_code == 200:
                if is_doc:
                    bytes = utils.convert_doc_to_pdf(file.content, container_url=DOC_TO_PDF_CONTAINER_URL)
                bytes = BytesIO(bytes)
            else:
                raise utils.PleaseRetryException()
            try:
                merger.append(bytes)
            except PyPDF2.errors.PdfReadError:
                raise utils.PleaseRetryException()

        pdf_bytes = BytesIO()
        merger.write(pdf_bytes)
        return pdf_bytes.getvalue()


    @utils.retryable(max_retries=9, sleeptime=20)
    def search_trf1_process_documents(self, browser, title):
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.common.by import By
        from selenium.common.exceptions import TimeoutException

        browser.get(TRF1_SEARCH_LINK)
        browser.fill_in('fPP:numProcesso-inputNumeroProcessoDecoration:numProcesso-inputNumeroProcesso', title)
        browser.driver.implicitly_wait(20)
        WebDriverWait(browser.driver, 20).until(EC.element_to_be_clickable((By.ID,'fPP:searchProcessos'))).click() 
        try:
            WebDriverWait(browser.driver, 20).until(EC.presence_of_element_located((By.XPATH, '//a[@title="Ver Detalhes"]')))
        except TimeoutException:
            return False
        link = browser.bsoup().find_all('a', attrs={'title':'Ver Detalhes'})
        if len(link) != 1:
            return False
        link = re.search(r".*\(\'Consulta pública\','(.*?)\'\)",link[0]['onclick']).group(1)
        browser.get(f'https://pje2g.trf1.jus.br{link}')
        browser.driver.implicitly_wait(20)
        return True

    @utils.retryable(max_retries=9, sleeptime=20)
    def download_pdf(self, browser):
        """Download PDF as bytes when browser is in the page where the "Gerar PDF" button is available"""
        import requests
        link_container =  browser.bsoup().find('a',id='j_id47:downloadPDF')
        DATA_PATTERN = r".*\'ca\'\:\'(?P<ca>.*)\',\'idProcDocBin\'\:\'(?P<idProcDocBin>\d+)\'.*"
        page_data = re.search(DATA_PATTERN, link_container['onclick']).groupdict()

        cookies = {
            'JSESSIONID': browser.get_cookie('JSESSIONID')
        }

        data = {
            'j_id47': 'j_id47',
            'javax.faces.ViewState': browser.bsoup().find("input", {"type": "hidden", "name":"javax.faces.ViewState"})['value'],
            'j_id47:downloadPDF': 'j_id47:downloadPDF',
            'ca': page_data['ca'],
            'idProcDocBin': page_data['idProcDocBin'],
        }

        headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:99.0) Gecko/20100101 Firefox/99.0',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            # 'Accept-Encoding': 'gzip, deflate, br',
            'Origin': 'https://pje2g.trf1.jus.br',
            'Connection': 'keep-alive',
            'Referer': f'https://pje2g.trf1.jus.br/consultapublica/ConsultaPublica/DetalheProcessoConsultaPublica/documentoSemLoginHTML.seam?ca={page_data["ca"]}&idProcessoDoc={page_data["idProcDocBin"]}',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
        }
        response = requests.post(PDF_URL, cookies=cookies, headers=headers, data=data)
        if response.status_code == 200 and len(response.text) > 50:
            return response.content
        else:
            raise utils.PleaseRetryException()

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
