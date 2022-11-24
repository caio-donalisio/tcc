import base
import math
import pendulum
import celery
import utils
from logconfig import logger_factory, setup_cloud_logger
import click
from app import cli, celery
import re
import time
import os

DEBUG = True
SITE_KEY = '6Lf778wZAAAAAKo4YvpkhvjwsrXd53EoJOWsWjAY' # k value of recaptcha, found inside page
WEBSITE_URL = 'http://sagror.prefeitura.sp.gov.br/ManterDecisoes/pesquisaDecisoesCMT.aspx'
CMTSP_DATE_FORMAT = 'DDMMYYYY'
CRAWLER_DATE_FORMAT = 'YYYY-MM-DD'
DATE_PATTERN = re.compile(r'\d{2}/\d{2}/\d{4}')
FILES_PER_PAGE = 10
PDF_URL = ''
CMTSP_SEARCH_LINK = ''

DEFAULT_HEADERS =  {
            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:106.0) Gecko/20100101 Firefox/106.0',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            # 'Accept-Encoding': 'gzip, deflate',
            'Referer': 'http://sagror.prefeitura.sp.gov.br/ManterDecisoes/pesquisaDecisoesCMT.aspx',
            'Connection': 'keep-alive',
            # Requests sorts cookies= alphabetically
            # 'Cookie': 'ASP.NET_SessionId=pl0zpa11y03mwmshi1inotwv; SWCookieConfig={"aceiteSessao":"S","aceitePersistentes":"N","aceiteDesempenho":"N","aceiteEstatisticos":"N","aceiteTermos":"S"}',
            'Upgrade-Insecure-Requests': '1',
        }


logger = logger_factory('cmtsp')

class CMTSPClient:

    def __init__(self):
        import browsers
        self.browser = browsers.FirefoxBrowser(headless=not DEBUG)

    @utils.retryable(max_retries=9, sleeptime=20)
    def setup(self):
        self.browser.get(WEBSITE_URL)

    @property
    def page_searched(self):
        return bool(self.browser.bsoup().find(name='span', text=re.compile(r'^\d+$')))

    def search_is_over(self, current_page):
        return bool(
            not self.browser.bsoup().find('a',attrs={'href':"__doPostBack('grdPesquisaDecisoes$ctl14$ctl11','')"}) and \
            not self.browser.bsoup().find('a', text=f'{current_page + 1}')
        )

    @utils.retryable(max_retries=9, sleeptime=20)
    def make_search(self,filters):
        import captcha
        self.browser.driver.implicitly_wait(10)

        #ACEITA COOKIES
        self.browser.driver.execute_script('''
        document.querySelector("prodamsp-componente-consentimento").shadowRoot.querySelector("input[class='cc__button__autorizacao']").click()''')

        #PREENCHE DATAS
        self.browser.driver.implicitly_wait(10)
        self.browser.fill_in('txtDtInicio',pendulum.parse(filters.get('start_date')).format(CMTSP_DATE_FORMAT))
        self.browser.fill_in('txtDtFim',pendulum.parse(filters.get('end_date')).format(CMTSP_DATE_FORMAT))

        #RECAPTCHA
        # captcha.solve_recaptcha(self.browser, logger, SITE_KEY)
        
        #CLICK 'PESQUISAR'
        self.browser.driver.find_element_by_id('btnPesquisar').click()
        self.browser.driver.implicitly_wait(10)
        return bool(self.browser.bsoup().find('td', text='Ementa'))


    @utils.retryable(max_retries=9, sleeptime=20)
    def count(self, filters):
        #Count not available
        return 0

    @utils.retryable(max_retries=9, sleeptime=20)
    def fetch(self, filters, page=1):
        def get_current_page():
            return int(self.browser.bsoup().find(name='span', text=re.compile(r'^\d+$')).text)

        while not self.page_searched:
            self.setup()
            self.make_search(filters)

        rows = True
        while get_current_page() != page:
            self.browser.driver.implicitly_wait(20)
            if not self.page_searched or not rows:
                self.make_search(filters)
            self.browser.driver.implicitly_wait(20)
            if page != get_current_page():
                #Checks if target page and current page belong to same 10 page block
                if (page-1)//10  != (get_current_page()-1)//10:
                    if page > get_current_page():
                        self.browser.driver.execute(f"""__doPostBack('grdPesquisaDecisoes$ctl14$ctl11','')""")
                    else:
                        self.browser.driver.execute(f"""__doPostBack('grdPesquisaDecisoes$ctl14$ctl00','')""")
                else:
                    self.browser.driver.find_element_by_xpath(f'//a[text()="{page}"]').click()
        return self.browser.bsoup()

    def get_pdf_session_id(self, tr):
        self.browser.driver.find_element_by_id(tr.a['id']).click()
        self.browser.driver.implicitly_wait(3)
        return self.browser.get_cookie('ASP.NET_SessionId')
        # self.browser.click()
        ...

    def fetch_pdf(self, session_id):
        import requests
        #self.browser.get_cookie('ASP.NET_SessionId')
        cookies = {
            'ASP.NET_SessionId': session_id,
            'SWCookieConfig': '{"aceiteSessao":"S","aceitePersistentes":"N","aceiteDesempenho":"N","aceiteEstatisticos":"N","aceiteTermos":"S"}',
        }
        headers = DEFAULT_HEADERS
        response = requests.get('http://sagror.prefeitura.sp.gov.br/ManterDecisoes/VisualizarArquivo.aspx', cookies=cookies, headers=headers)
        return response.content



class CMTSPCollector(base.ICollector):

    def __init__(self, client, filters):
        self.client = client
        self.filters = filters

    @utils.retryable(max_retries=9, sleeptime=20)
    def count(self):
        return self.client.count(self.filters)

    @utils.retryable(max_retries=9, sleeptime=20)
    def chunks(self):
        ranges = utils.timely(
            pendulum.parse(self.filters['start_date']), 
            pendulum.parse(self.filters['end_date']), 
            unit='weeks', step=1)

        for start_date, end_date in reversed(list(ranges)):
            keys =\
                {'start_date': start_date.to_date_string(),
                'end_date'  : end_date.to_date_string()}

            yield CMTSPChunk(
                keys=keys,
                client=self.client,
                filters=self.filters,
                prefix=f'{start_date.year}/{start_date.month:02d}/'
                )


class CMTSPChunk(base.Chunk):
    def __init__(self, keys, client, filters, prefix):
        super(CMTSPChunk, self).__init__(keys, prefix)
        self.client  = client
        self.filters = filters

    @utils.retryable(max_retries=3)
    def rows(self):

        # self.client._get_cookies()
        page = 1
        self.client.setup()
        success = self.client.make_search(self.keys)
        if success:
            trs = []
            while True:
                soup = self.client.fetch(self.filters, page)
                trs = self._get_page_trs(soup)
                for tr in trs:
                    ementa_hash = utils.get_content_hash(tr, [{'name':'td'}])
                    process_code = utils.extract_digits(tr.find('td').text)
                    filepath = f"{process_code}_{ementa_hash}"
                    yield self.fetch_act_meta(tr, filepath)
                    act_session_id = self.client.get_pdf_session_id(tr)
                    yield self.fetch_act_pdf(act_session_id, filepath)
                    

                    # pdf_content = self.fetch_act_pdf(tr_session_id)
                    # meta_content = self.fetch_act_meta(tr)
                    # ementa_hash = utils.get_content_hash(tr, [{'name':'td'}])
                    # process_code = utils.extract_digits(tr.find('td').text)
                    # file_path = f"{process_code}_{ementa_hash}"
                    # self.fetch_act(filepath)
                    # self.fetch_act_pdf(filepath)
                    # pdf_content = fetch_pdf(tr_session_id)

                    ...
                # trs.extend(self._get_page_trs(soup))
                if self.client.search_is_over(page):
                    break
                page += 1
            

            # for tr in trs:
            #     yield self.fetch_act(tr)
            #     time.sleep(0.1)

    @utils.retryable(max_retries=3)
    def fetch_act_meta(self, tr, filepath):
        
        # tds = tr.find_all('td')
        if self.keys['start_date']==self.keys['end_date']:
            tr.insert_tag('td',text=f"Data de Julgamento: {self.keys['start_date']}")
            # tds.append(f'<td>Data de Julgamento: {self.keys["start_date"]}<td>')
        else:
            logger.info('Skipped session date injection - key is not a single day')
        # return ''.join(tds)


        # pdf_href = tds[7].a['href']
        return [
            base.Content(content=tr.prettify(), dest=f"{filepath}.html",
            content_type='text/html'),
            # self.fetch_pdf(pdf_href, act_id, publication_date)
        ]

    @utils.retryable(max_retries=3)
    def fetch_act_pdf(self, session_id, filepath):
        import requests
        #self.browser.get_cookie('ASP.NET_SessionId')
        cookies = {
            'ASP.NET_SessionId': session_id,
            'SWCookieConfig': '{"aceiteSessao":"S","aceitePersistentes":"N","aceiteDesempenho":"N","aceiteEstatisticos":"N","aceiteTermos":"S"}',
        }
        headers = DEFAULT_HEADERS
        response = requests.get('http://sagror.prefeitura.sp.gov.br/ManterDecisoes/VisualizarArquivo.aspx', cookies=cookies, headers=headers)

        return [
            base.Content(content=response.content, 
            dest=f"{filepath}.pdf", content_type='application/pdf')]

        # return response.content

    # @utils.retryable(max_retries=3)
    # def fetch_pdf(self, pdf_href, act_id, publication_date):
    #     data = self.client.data
    #     data['__EVENTTARGET'] = utils.find_between(
    #         pdf_href, start='WebForm_PostBackOptions\("', end='"')
    #     data['__EVENTARGUMENT'] = ''
    #     pdf_response = self.client.requester.post(self.client.base_url, data=data)
    #     pdf_filepath = utils.get_filepath(
    #         date=publication_date, filename=act_id, extension='pdf')
    #     return base.Content(content=pdf_response.content, dest=pdf_filepath,
    #         content_type='application/pdf')

    def _get_page_trs(self, soup):
        trs = soup.find_all('tr')
        return trs[1:len(trs)-1]


@celery.task(queue='crawlers.cmtsp', default_retry_delay=5 * 60,
             autoretry_for=(BaseException,))
def cmtsp_task(**kwargs):
    import utils
    setup_cloud_logger(logger)

    from logutils import logging_context

    with logging_context(crawler='cmtsp'):
        output = utils.get_output_strategy_by_path(
            path=kwargs.get('output_uri'))
        logger.info(f'Output: {output}.')

        query_params = {
            'start_date': kwargs.get('start_date'),
            'end_date': kwargs.get('end_date'),
        }

        collector = CMTSPCollector(client=CMTSPClient(), filters=query_params)
        handler = base.ContentHandler(output=output)
        snapshot = base.Snapshot(keys=query_params)

        base.get_default_runner(
            collector=collector,
            output=output,
            handler=handler,
            logger=logger,
            max_workers=8) \
            .run(snapshot=snapshot)


@cli.command(name='cmtsp')
@click.option('--start-date',
  default=utils.DefaultDates.THREE_MONTHS_BACK.strftime("%Y-%m-%d"),
  help='Format YYYY-MM-DD.',
)
@click.option('--end-date'  ,
  default=utils.DefaultDates.NOW.strftime("%Y-%m-%d"),
  help='Format YYYY-MM-DD.',
)
@click.option('--output-uri',    default=None,     help='Output URI (e.g. gs://bucket_name')
@click.option('--enqueue',    default=False,    help='Enqueue for a worker', is_flag=True)
@click.option('--split-tasks',
              default=None, help='Split tasks based on time range (weeks, months, days, etc) (use with --enqueue)')
def cmtsp_command(**kwargs):
    if kwargs.get('enqueue'):
        if kwargs.get('split_tasks'):
            start_date = pendulum.parse(kwargs.get('start_date'))
            end_date = pendulum.parse(kwargs.get('end_date'))
            for start, end in utils.timely(start_date, end_date, unit=kwargs.get('split_tasks'), step=1):
                task_id = cmtsp_task.delay(
                    start_date=start.to_date_string(),
                    end_date=end.to_date_string(),
                    output_uri=kwargs.get('output_uri'))
                print(
                    f"task {task_id} sent with params {start.to_date_string()} {end.to_date_string()}")
        else:
            cmtsp_task.delay(**kwargs)
    else:
        cmtsp_task(**kwargs)
