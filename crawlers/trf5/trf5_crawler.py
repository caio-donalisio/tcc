import base
import math
import json
import pendulum
import celery
import utils
import time
import os
from logconfig import logger_factory, setup_cloud_logger
import click
from app import cli, celery
import requests
import re
import browsers
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup

logger = logger_factory('trf5')

SOURCE_DATE_FORMAT='DD/MM/YYYY'
DEFAULT_HEADERS = {
    'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
                        ' AppleWebKit/537.36 (KHTML, like Gecko)'
                        ' Chrome/92.0.4515.131 Safari/537.36 Edg/92.0.902.67'),
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Language': 'en-US,en;q=0.9,pt;q=0.8',
    'Connection': 'keep-alive',
    'Referer': 'https://julia-pesquisa.trf5.jus.br/julia-pesquisa/',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'X-Requested-With': 'XMLHttpRequest',
}                        

def merged_with_default_filters(start_date, end_date):
    return {
        'draw': '1',
        'columns[0][data]': 'codigoDocumento',
        'columns[0][name]': '',
        'columns[0][searchable]': 'true',
        'columns[0][orderable]': 'false',
        'columns[0][search][value]': '',
        'columns[0][search][regex]': 'false',
        'start': '0',
        'length': '10',
        'search[value]': '',
        'search[regex]': 'false',
        'pesquisaLivre': '',
        'numeroProcesso': '',
        'orgaoJulgador': '',
        'relator': '',
        'dataIni': f'{start_date}',
        'dataFim': f'{end_date}',
    }


class TRF5Client:

    def __init__(self):
        self.url = 'https://julia-pesquisa.trf5.jus.br/julia-pesquisa/api/documentos:dt'

    @utils.retryable(max_retries=3)
    def count(self, filters):
        result = self.fetch(filters, page=1)
        return result['recordsTotal']

    def get_cookie(self, cookie_name):
        value = None
        cookies = self.driver.get_cookies()
        for cookie in cookies:
            if cookie.get('name') == cookie_name:
                value = cookie['value']
        if cookie is None:
            raise Exception(f'Cookie not found: {cookie_name}')
        return value

    @utils.retryable(max_retries=3)
    def fetch(self, filters, page=1, per_page=10):
        try:
            filters['start'] = (page * per_page) - per_page
            filters['length'] = per_page

            return requests.get(self.url,
                                params=filters,
                                headers=DEFAULT_HEADERS
                                ).json()

        except Exception as e:
            logger.error(f"page fetch error params: {filters}")
            raise e


class TRF5Collector(base.ICollector):

    def __init__(self, client, filters, browser):
        self.client  = client
        self.filters = filters
        self.browser = browser

    def count(self):
        return self.client.count(merged_with_default_filters(**self.filters))

    def chunks(self):
        total = self.count()
        pages = math.ceil(total/10)

        for page in range(1, pages + 1):
            yield TRF5Chunk(
                keys={
                    **self.filters  , **{'page': page}
                },
                prefix='',
                filters=self.filters,
                page=page,
                client=self.client,
                browser=self.browser
            )


class TRF5Chunk(base.Chunk):

    def __init__(self, keys, prefix, filters, page, client, browser):
        super(TRF5Chunk, self).__init__(keys, prefix)
        self.filters = filters
        self.page = page
        self.client = client
        self.browser = browser

    def rows(self):
        result = self.client.fetch(merged_with_default_filters(**self.filters),self.page)
        for _, record in enumerate(result['data']):

            session_at = pendulum.parse(record['dataJulgamento'])
            base_path   = f'{session_at.year}/{session_at.month:02d}'

            codigo = re.sub("\:", "-", record['codigoDocumento'])
            numero = record['numeroProcesso']

            dest_record = f"{base_path}/doc_{numero}_{codigo}.json"
            dest_report = f"{base_path}/doc_{numero}_{codigo}.html"
            report_url = None
            content_type_report = "text/html"

            if re.search('www4.trf5.jus.br\/processo', record['url']):
                report_url = self._get_report_url_from_trf5(record)
                if report_url is None:
                    report_url = self._get_report_url_from_trf5(record, digits=2)

                if report_url is not None:
                    dest_report = f"{base_path}/doc_{numero}_{codigo}.pdf"
                    content_type_report = "application/pdf"
            else:
                report_url = self._get_report_url(record)

            if report_url is None:
                logger.warn(f"Not found 'Inteiro Teor' for judgment {record['numeroProcesso']}")
                yield [
                    base.Content(content=json.dumps(record),dest=dest_record,
                        content_type='application/json'),
                ]                    
            else:            
                yield [
                    base.Content(content=json.dumps(record),dest=dest_record,
                        content_type='application/json'),
                    base.ContentFromURL(src=report_url,dest=dest_report,
                        content_type=content_type_report)
                ]

    
    def _get_report_url_from_trf5(self, doc, digits=0):
        judgment_date = pendulum.parse(doc['dataJulgamento'])
        judgment_id = doc['numeroProcesso'][0:len(doc['numeroProcesso'])-digits]

        data = {
            'numproc': judgment_id,
        }
        baseURL = 'https://www4.trf5.jus.br'
        response = requests.post(f'{baseURL}/InteiroTeor/publicacoes.jsp', data=data)
        soup = BeautifulSoup(response.content, 'html.parser')
        table = soup.find('table', {'cellpadding': '5'})
        pattern = "(\d{2})\/(\d{2})\/(\d{4})\s*.*.pdf"
        trs = table.find_all('tr')
        links = []
        for tr in trs:
            match = re.search(pattern, tr.text)
            if match:
                doc_date = pendulum.parse(f'{match.group(3)}-{match.group(2)}-{match.group(1)}')
                days = doc_date.diff(judgment_date).in_days()
                a = tr.find('a')
                if days >= 0 and a:
                    links.append({
                        'days': days,
                        'url': f"{baseURL}{a.get('href')}"
                    })

        return self._get_judgment_doc_url_by_closest_date(links)

    
    def _get_report_url(self, doc):
        details_url = self._get_judgment_details_url(doc)
        return self._get_judgment_doc_url(details_url, doc)
    

    def _get_judgment_details_url(self, doc):
        self.browser.get(doc['url'])

        self.browser.wait_for_element(locator=(By.ID, 'consultaPublicaForm:captcha:captchaImg'), timeout=30)

        judgment_id = self._format_process_number(doc['numeroProcesso'])
        logger.info(judgment_id)
        process_input = self.browser.driver.find_element_by_id('consultaPublicaForm:Processo:ProcessoDecoration:Processo')  
        self.browser.driver.execute_script(f"arguments[0].value='{judgment_id}';", process_input)

        while not self.browser.is_text_present('Ver Detalhes', tag='img'):
            captcha_img = self.browser.driver.find_element_by_id('consultaPublicaForm:captcha:captchaImg')

            captcha_img_base64 = self.browser.driver.execute_script("""
                var ele = arguments[0];
                var cnv = document.createElement('canvas');
                cnv.width = ele.width; cnv.height = ele.height;
                cnv.getContext('2d').drawImage(ele, 0, 0);
                return cnv.toDataURL('image/jpeg').substring(23);    
                """, captcha_img)

            captcha_resolved = self._resolve_captcha(captcha_img_base64)
            captcha_input = self.browser.driver.find_element_by_id('consultaPublicaForm:captcha:j_id268:verifyCaptcha')
            captcha_input.send_keys(captcha_resolved)

            search_button = self.browser.driver.find_element_by_id('consultaPublicaForm:pesq')
            search_button.click()
            time.sleep(1)
        
            if not self.browser.is_text_present('Resposta incorreta'):
                self.browser.wait_for_element(locator=(By.ID, 'consultaPublicaList2:0:j_id315:j_id318'), timeout=30)
                doc_link = self.browser.driver.find_element_by_id('consultaPublicaList2:0:j_id315:j_id318')                                            
                return self._extract_judgment_detail_url(doc_link)

    
    def _format_process_number(self, value):
        value = "{:0>20}".format(int(value))
        return re.sub("(\d{7})(\d{2})(\d{4})(\d{1})(\d{2})(\d{4})",
                    "\\1-\\2.\\3.\\4.\\5.\\6",
                    value)

    
    def _resolve_captcha(self, captcha):
        api_key = os.getenv('CAPTCHA_API_KEY')

        post_data = {
            'action':'upload',
            'key': api_key,
            'captchatype': 2,
            'gen_task_id': f'{int(time.time())}',
            'file': captcha
        }

        captcha_api_url = 'http://fasttypers.org/Imagepost.ashx'
        logger.debug(f'(Captcha) POST {captcha_api_url}')
        r  = requests.post(captcha_api_url, data=post_data)
        logger.debug(f'(Captcha) Response: {r.text}')

        return r.text


    def _extract_judgment_detail_url(self, doc_link):
        event_data = doc_link.get_attribute('onclick')
        pattern = re.compile(r"openPopUp\('\d+popUpDetalhesProcessoConsultaPublica', '(.*)'\);")
        m = pattern.match(event_data)
        if m:
            return f"https://pje.trf5.jus.br{m.group(1)}"

        return None


    def _get_judgment_doc_url(self, url: str, doc):
        judgment_date = pendulum.parse(doc['dataJulgamento'])
        pattern = '(\d{2})\/(\d{2})\/(\d{4})\s(\d{2})\:(\d{2})\:(\d{2})\s- Inteiro Teor - Inteiro Teor do Acórdão'
        self.browser.get(url)
        self.browser.driver.maximize_window()
        self.browser.wait_for_element(locator=(By.ID, 'processoEvento'))

        slider = self.browser.driver.find_element(By.XPATH, "//div[contains(@class, 'rich-inslider-handler')]")
        self.browser.driver.execute_script("arguments[0].scrollIntoView()", slider);

        slider_total_pages_td = self.browser.driver.find_element(By.XPATH, "//td[contains(@class, 'rich-inslider-right-num')]")
        slider_total_pages = int(slider_total_pages_td.text)

        slider_page_input = self.browser.driver.find_element_by_id('j_id423:j_id424Input')
        slider_page = int(slider_page_input.get_attribute('value'))

        links = []
        while slider_page <= slider_total_pages:
            html = self.browser.driver.page_source
            soup = BeautifulSoup(html, features='html.parser')
            table = soup.find("table", {"id": "processoEvento"})
            tds = table.find_all("td", {"class": "rich-table-cell"})
            for td in tds:
                match = re.search(pattern, td.text)
                if match:
                    a = td.find('a')
                    doc_date = pendulum.parse(f'{match.group(3)}-{match.group(2)}-{match.group(1)}')
                    days = doc_date.diff(judgment_date).in_days()
                    if days >= 0 and a:
                        links.append({
                            'days': days,
                            'url': self._extract_url_from_event(a.get('onclick'))
                        })
            
            slider_page_input = self.browser.driver.find_element_by_id('j_id423:j_id424Input')
            self.browser.driver.execute_script("arguments[0].value = Number(arguments[0].value) + 1;", slider_page_input);
            slider_page = int(slider_page_input.get_attribute('value'))
            self.browser.driver.execute_script("A4J.AJAX.Submit('j_id423',event,{'similarityGroupingId':'j_id423:j_id425','actionUrl':'/pjeconsulta/ConsultaPublica/DetalheProcessoConsultaPublica/listView.seam','eventsQueue':'default','containerId':'j_id340','parameters':{'j_id423:j_id425':'j_id423:j_id425'} ,'status':'_viewRoot:status'} )");
            time.sleep(2)

        return self._get_judgment_doc_url_by_closest_date(links)

    
    def _get_judgment_doc_url_by_closest_date(self, links):
        if len(links) > 0:
            sorted_list = sorted(links, key=lambda d: d['days'])
            return sorted_list[0]['url']
        
        return None

    def _extract_url_from_event(self, event_data):
        pattern = re.compile(r"openPopUp\('PopUpDocumentoBin', '(.*)'\);")
        m = pattern.match(event_data)
        if m:
            return m.group(1)
        
        return None


@celery.task(queue='crawlers.trf5', default_retry_delay=5 * 60,
            autoretry_for=(BaseException,))
def trf5_task(**kwargs):
    import utils
    setup_cloud_logger(logger)

    from logutils import logging_context

    with logging_context(crawler='trf5'):
        output = utils.get_output_strategy_by_path(path=kwargs.get('output_uri'))
        logger.info(f'Output: {output}.')

        start_date = pendulum.parse(kwargs.get('start_date')).format(SOURCE_DATE_FORMAT)
        end_date = pendulum.parse(kwargs.get('end_date')).format(SOURCE_DATE_FORMAT)

        filters = {
            'start_date' :start_date,
            'end_date': end_date,
        }

        collector = TRF5Collector(
            client=TRF5Client(), 
            filters=filters,
            browser=browsers.FirefoxBrowser(headless=True)
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


@cli.command(name='trf5')
@click.option('--start-date',    prompt=True,      help='Format YYYY-MM-DD.')
@click.option('--end-date'  ,    prompt=True,      help='Format YYYY-MM-DD.')
@click.option('--output-uri',    default=None,     help='Output URI (e.g. gs://bucket_name')
@click.option('--enqueue'   ,    default=False,    help='Enqueue for a worker'  , is_flag=True)
@click.option('--split-tasks',
  default=None, help='Split tasks based on time range (weeks, months, days, etc) (use with --enqueue)')
def trf5_command(**kwargs):
  if kwargs.get('enqueue'):
    if kwargs.get('split_tasks'):
      start_date = pendulum.parse(kwargs.get('start_date'))
      end_date = pendulum.parse(kwargs.get('end_date'))
      for start, end in utils.timely(start_date, end_date, unit=kwargs.get('split_tasks'), step=1):
        task_id = trf5_task.delay(
          start_date=start.to_date_string(),
          end_date=end.to_date_string(),
          output_uri=kwargs.get('output_uri'))
        print(f"task {task_id} sent with params {start.to_date_string()} {end.to_date_string()}")
    else:
      trf5_task.delay(**kwargs)
  else:
    trf5_task(**kwargs)