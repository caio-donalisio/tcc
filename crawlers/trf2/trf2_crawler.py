import time
import requests
import pendulum
import utils
import random
import logging
from tqdm import tqdm
from slugify import slugify
import hashlib

import browsers
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

import click
from app import cli, celery
from logconfig import logger_factory


class TRF2:
  def __init__(self, params, output, logger, **options):
    self.params = params
    self.output = output
    self.logger = logger
    self.options = (options or {})
    self.header_generator = utils.HeaderGenerator(
      origin='https://www10.trf2.jus.br', xhr=True)
    self.session = requests.Session()
    self.browser = browsers.FirefoxBrowser(headless=True)

  def run(self):
    import concurrent.futures

    total_records = self.count()
    self.logger.info(f'Expects {total_records} records.')
    records_fetch = 0

    tqdm_out = utils.TqdmToLogger(self.logger, level=logging.INFO)
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
      with tqdm(total=total_records, file=tqdm_out) as pbar:
        for chunk in self.chunks():
          if chunk.commited():
            chunk_records  = chunk.get_value('records')
            records_fetch += chunk_records
            pbar.set_postfix(chunk.params)
            pbar.update(chunk_records)
            self.logger.debug(f"Chunk {chunk.hash} already commited ({chunk_records} records) -- skipping.")
            continue

          chunk_records = 0
          futures = []
          for doc_short, doc_full, pdf in chunk.rows():
            chunk_records += 1
            futures.extend([
              executor.submit(self.persist, doc_short, content_type='text/html'),
              executor.submit(self.persist, doc_full , content_type='text/html'),
              executor.submit(self.handle_pdf, pdf)
            ])

          for future in concurrent.futures.as_completed(futures):
            future.result()

          chunk.set_value('records', chunk_records)
          chunk.commit()
          records_fetch += chunk_records
          pbar.set_postfix(chunk.params)
          pbar.update(chunk_records)
          self.logger.debug(f'Chunk {chunk.hash} ({chunk_records} records) commited.')

      self.logger.info(f'Expects {total_records}. Fetched {records_fetch}.')

  def persist(self, doc, **kwargs):
    self.output.save_from_contents(
      filepath=doc['dest'],
      contents=doc['source'],
      **kwargs)

  def handle_pdf(self, pdf):
    from tasks import download_from_url
    if pdf['url'] is None:
      return

    download_args = dict(
      url=pdf['url'],
      dest=pdf['dest'],
      output_uri=self.output.uri,
      headers=self.header_generator.generate(),
      content_type='application/pdf',
      write_mode='wb',
      ignore_not_found=True,
      override=False)

    if self.options.get('skip_pdf', False) is False:
      if self.options.get('pdf_async', False):
        download_from_url.delay(**download_args)
      else:
        download_from_url(**download_args)

  def chunks(self):
    ranges = list(utils.timely(
      self.params['start_date'], self.params['end_date'], unit='days', step=3))
    for start_date, end_date in reversed(ranges):
      chunk_params = {
        'start_date': start_date.to_date_string(),
        'end_date'  : end_date.to_date_string()
      }
      yield utils.Chunk(params=chunk_params, output=self.output,
        rows_generator=self.rows(start_date=start_date, end_date=end_date))

  def rows(self, start_date, end_date):
    # Check whether we can trust pagination
    base_query = {
      'start_date': start_date.start_of('day').to_date_string(),
      'end_date'  : end_date.start_of('day').to_date_string()
    }
    page = self._get_search_page_for_query({**base_query, 'offset': 0})

    total = page.count()
    if total > 900:
      count = 0
      for filters in page.filters():
        query   = {'extra_query_params': filters['query_param'], **base_query}
        partial = 0
        for row in self.rows_paginated(query):
          partial += 1
          yield row
        count += partial
      if count != total:
        self.logger.warn(f'Count does not match, fetch {count} expects {total}.')
    else:
      yield from self.rows_paginated(base_query)

  def rows_paginated(self, query):
    offset = 0
    while True:
      page = self._get_search_page_for_query({**query, 'offset': offset})
      # browser = page.browser
      if not page.has_results():
        break

      results = page.results()
      offset += len(results)

      for result in results:
        item_html = result.prettify()

        # get `data de publicacao`
        data_els = result.find_all('div', {'class': 'data-relator'})
        assert len(data_els) == 1
        data_raw = data_els[0].find_all('span', {'class': 'valor'})[1].get_text()
        _, month, year = data_raw.split('/')

        # get a possible name for the file.
        doc_title = result.find_all('span', {'class': 'number_link'}, resursive=False)
        assert len(doc_title) == 1
        # as we can trust this doc_id to be unique -- check whether the content is the same.
        doc_id   = slugify(doc_title[0].get_text())
        filename =\
          f"{doc_id}__{hashlib.sha1(item_html.encode()).hexdigest()}"

        doc_short = {
          'source': result.prettify(encoding='cp1252'),
          'dest'  :  f'{year}/{month}/{filename}.html',
        }
        doc_full = {
          'source': '',
          'dest'  :  f'{year}/{month}/{filename}_full.html',
        }
        pdf = {
          'url'   : None,
          'dest'  :  f'{year}/{month}/{filename}.pdf',
        }

        links = result.find_all('a', {'class': 'font_bold'})
        for link in links:
          # pdf (available on listing)
          if link.get_text() == 'Inteiro teor':
            pdf['url'] = link['href']

          # content
          if link.get_text() == 'Pré-visualização':
            preview_path = link['href']
            url = f'https://www10.trf2.jus.br/consultas/{preview_path}'
            doc_page = self.fetch_doc(url=url)
            time.sleep(random.uniform(0.1, 0.2))
            doc_full['source'] = doc_page['content']
            if doc_page['pdf_url']:
              pdf['url'] = doc_page['pdf_url']

        yield doc_short, doc_full, pdf

      if not page.has_next():
        break
      time.sleep(random.uniform(0.1, 0.2))

  @utils.retryable(max_retries=9)   # type: ignore
  def fetch_doc(self, url):
    response = requests.get(url, allow_redirects=True, verify=False, timeout=15)

    soup = utils.soup_by_content(response.text)
    pdf_url = None
    for link in utils.soup_by_content(response.text).find_all('a'):
      if 'inteiro teor' in link.get_text().lower():
        pdf_url = link['href']

    return {'content': soup.prettify(), 'pdf_url': pdf_url}

  def count(self):
    total = 0
    for start_date, end_date in \
      utils.timely(self.params['start_date'], self.params['end_date'], unit='years', step=1):
      total += self._get_search_page_for_query(query={
        'start_date': start_date.start_of('day').to_date_string(),
        'end_date'  : end_date.start_of('day').to_date_string(),
        'offset'    : 0}) \
      .count()
    return total

  def _get_search_page_for_query(self, query):
    return SearchPageSelenium(browser=self.browser, query=query, logger=self.logger)


class SearchPageSelenium:
  def __init__(self, browser, query, logger):
    self.logger = logger
    self.browser = browser
    self._text = self._perform_query(query)
    self._soup = utils.soup_by_content(self._text)
    self._uls  =\
      self._soup.find_all('ul', {'class': 'ul-resultados'})

  @utils.retryable(max_retries=3)   # type: ignore
  def _perform_query(self, query):
    import urllib.parse

    q_string = '+inmeta%3Agsaentity_BASE%3DInteiro%2520Teor'
    if query.get('extra_query_params'):
      param = query['extra_query_params'][0]
      q_string = f'{q_string}+inmeta%3ADescOrgaoJulgador%3D{urllib.parse.quote(param)}'
      self.logger.debug(f'Using `extra_query_params` filters..')

    query_url = 'https://www10.trf2.jus.br/consultas/?proxystylesheet=v2_index&getfields=*&entqr=3&lr=lang_pt&ie=UTF-8&oe=UTF-8&requiredfields=(-sin_proces_sigilo_judici:s).(-sin_sigilo_judici:s)&sort=date:A:S:d1&entsp=a&adv=1&base=JP-TRF&ulang=&access=p&entqrm=0&wc=200&wc_mc=0&ud=1&client=v2_index&filter=0&as_q=inmeta:DataDispo:daterange:{start_date}..{end_date}&q={q}&start={offset}&num=1&site=v2_jurisprudencia'.format(
      start_date=query['start_date'], end_date=query['end_date'], offset=query['offset'], q=q_string)

    self.browser.get(query_url)
    WebDriverWait(self.browser.driver, 60) \
      .until(EC.presence_of_element_located((By.ID, 'resultados')))
    self.click_to_show_ementas()
    return self.browser.page_source()

  @utils.retryable()
  def click_to_show_ementas(self):
    select_objects = self.browser.driver.find_elements(By.XPATH,'//*[text()="Ver texto completo"]')
    for n, link in enumerate(select_objects):
      self.browser.driver.implicitly_wait(10)
      self.browser.driver.execute_script("arguments[0].scrollIntoView(true);", link)
      link.click()
      self.browser.driver.implicitly_wait(10)
    self.browser.driver.execute_script("window.scrollTo(0,99999999)");




  @property
  def text(self):
    return self._text

  @property
  def html(self):
    return self._text

  def has_results(self):
    return len(self._uls) == 1

  def has_next(self):
    last_page_link =\
      self._soup.find_all('a', {'class': 'pagination-link'}, string='Último')
    return len(last_page_link) > 0

  def results(self):
    assert len(self._uls) == 1
    return self._uls[0].find_all('li', recursive=False)

  def filters(self):
    import urllib.parse as urlparser

    # expand filters
    MORE_BUTTON_ID = 'more_attr_3'
    more_button = self._soup.find(id=MORE_BUTTON_ID)
    if more_button:
      self.browser.driver.execute_script("arguments[0].scrollIntoView(true);",
        self.browser.driver.find_element(By.ID, MORE_BUTTON_ID))
      self.browser.click(more_button)
      WebDriverWait(self.browser.driver, 60) \
        .until(EC.visibility_of_element_located((By.ID, "less_attr_3")))

      # the update instance-level source
      self._text = self.browser.page_source()
      self._soup = utils.soup_by_content(self._text)

    def extract_param(a):
      if a.get('href'):
        qs = urlparser.parse_qs(a['href'].replace('&amp;', '&')[1:])
        if qs.get('q'):
          params = qs['q'][0].split(' ')
          return [v.strip().split('=')[-1]
            for v in params if 'inmeta:DescOrgaoJulgador' in v]

    divs = self._soup.find_all('div', {'class': 'filtros_dyNav'})
    all_params = []
    for div in divs:
      for a in div.find_all('a'):
        query_param = extract_param(a)
        if query_param:
          all_params.append({'query_param': query_param})
    return all_params

  def count(self):
    import re
    facets = self._soup.find_all('a', {'title': 'Inteiro Teor'})
    if len(facets) == 0:
      return 0
    match = re.match(r'.*\((\d+)\)$', facets[0].get_text())
    if match:
      return int(match.group(1))
    return 0


@celery.task(queue='crawlers', rate_limit='1/h', default_retry_delay=30 * 60,
             autoretry_for=(Exception,))
def trf2_task(start_date, end_date, output_uri, pdf_async, skip_pdf):
  start_date, end_date =\
    pendulum.parse(start_date), pendulum.parse(end_date)

  output = utils.get_output_strategy_by_path(path=output_uri)
  logger = logger_factory('trf2')
  logger.info(f'Output: {output}.')

  crawler = TRF2(params={
    'start_date': start_date, 'end_date': end_date
  }, output=output, logger=logger, pdf_async=pdf_async, skip_pdf=skip_pdf)
  crawler.run()


@cli.command(name='trf2')
@click.option('--start-date',
  default=utils.DefaultDates.BEGINNING_OF_YEAR_OR_SIX_MONTHS_BACK.strftime("%Y-%m-%d"),
  help='Format YYYY-MM-DD.',
)
@click.option('--end-date'  ,
  default=utils.DefaultDates.NOW.strftime("%Y-%m-%d"),
  help='Format YYYY-MM-DD.',
)
@click.option('--output-uri', default=None,  help='Output URI (e.g. gs://bucket_name')
@click.option('--pdf-async' , default=False, help='Download PDFs async'   , is_flag=True)
@click.option('--skip-pdf'  , default=False, help='Skip PDF download'     , is_flag=True)
@click.option('--enqueue'   , default=False, help='Enqueue for a worker'  , is_flag=True)
def trf2_command(start_date, end_date, output_uri, pdf_async, skip_pdf, enqueue):
  args = (start_date, end_date, output_uri, pdf_async, skip_pdf)
  if enqueue:
    trf2_task.delay(*args)
  else:
    trf2_task(*args)
