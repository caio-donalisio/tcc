import re
import pendulum
import click
import requests
from typing import List
import bs4
from app.crawlers import base, utils, logutils
import time
import random

from app.crawler_cli import cli
from app.celery_run import celery_app as celery
from app.crawlers.logconfig import logger_factory, setup_cloud_logger
logger = logger_factory('stjmono')
# import fake_useragent, fake_headers

NOW = pendulum.now().to_datetime_string()
DATE_FORMAT_1 = 'YYYYMMDD'
DATE_FORMAT_2 = 'DD/MM/YYYY'
DEFAULT_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:108.0) Gecko/20100101 Firefox/108.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    # 'Accept-Encoding': 'gzip, deflate, br',
    'Referer': 'https://www.bing.com/',
    'Connection': 'keep-alive',
    # 'Cookie': 'JSESSIONID=XctixzZ3A1pao6DdOdzbqa_O54uPC9v9J5a6T0rI.svlp-jboss-02; f5_cspm=1234; TS01603393=016a5b38333e86e67759d5e6bc10a299aa735a89d845c2840bdac685740989e08c584ea68b1d41771fdd4a5156538d045362f3b8c4; TS0133e456=0132b058474f7cad5f96037014bea6531d2687d73def7426a15e33cb0abd65ac96527a02c53b2a8f591cecfd96a49c0673e01d34b9672312d87b85bb897d84f96d07d1dae458235116362bc38cea84b45853c2831bf0f3803c1f0e132b9cb7f8b55542cbb8; BIGipServerpool_svlp-jboss_scon=1073981632.36895.0000; TS01e2b0fb=016a5b3833fe4dc10b307602f3520399bdd72681eba4cb93634b208aa8070bca91a417f136da26311d0e5a4ebe7e6dd4744c578db8; TS0165095f=0132b0584736aa8bef8100e24865e72c27e3d7123aef7426a15e33cb0abd65ac96527a02c53b2a8f591cecfd96a49c0673e01d34b9aa2f8cafb34ac35669d98d0f92cd5f0f19d4647fa2d94ba41fba2208780f8970; TS013f88e9=0132b058479a7302abfb576dc7d622faab38c526b6ef7426a15e33cb0abd65ac96527a02c503e5de9e232bf2e94a214c8a1b96d6b1fe99d4e22c3ce382a7c854aa3f046a01; BIGipServerpool_wserv=973081866.20480.0000; TS01dc523b=016a5b38338383f6d76afb9388f3614cf51c8418c83f87a486065e063cd0d47dbbceec90f2f4ef17c036783204dd9862d562b34459; TS0122e3df=0132b058470a45f18ca8e2f02b33f6a452a980eb102c2d9a237cfe45789675057cd0dd389fbb8c613ec3f42cfb30bfa27a05870f009cbef87ca568627044ab6d4b34167584; _ga=GA1.3.1007550547.1674056737; _gid=GA1.3.1792403011.1674056737; _gat_UA-179972319-1=1',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'cross-site',
}

DEFAULT_HEADERS_COUNT = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6,ja;q=0.5',
    'Connection': 'keep-alive',
    # 'Cookie': 'JSESSIONID=pMaSxws6hKAkFmc706-uANBoSpB3a4LwY4s9jqxW.svlp-jboss-01; f5_cspm=1234; TS01603393=016a5b383375f3a3b5799f014b59c312a91ad2373991c85904fd298986034a2bc8a0f70941dce93412ff88b2288666e472e7564967; TS0133e456=0132b05847acf67bfeb8c8661d345d4f7800d4d746dc8f6dea204253e47be5bd684e1e03e2fbe7c27a3e773b0c8250a870726f0e5b56ca42c934b24941e32d49a2fbea1e100e795b62a14d01346b892784b2b2f093; _ga=GA1.3.84998084.1664279080; _gid=GA1.3.209578465.1674152937; BIGipServerpool_svlp-jboss_scon=1057204416.36895.0000; TS0165095f=0132b058478dbb8e8369ecf2c55ab89256789e9f68dc8f6dea204253e47be5bd684e1e03e2ccf4780198af2cc4c69bff6eb092bd8cf5dead30dd320b8ac04202cb66fa037064648f322fba922452c082ecdeb4d93b; _gat_UA-179972319-1=1; TS01e2b0fb=016a5b38336cac492a564ddb1a01eef41137edb69577763a22c3c141c108d9250da2f56cbef87963f99f07f406493a4cbb98559db2; TS013f88e9=0132b0584733e6e216c26732a714b70880c7e9e829dc8f6dea204253e47be5bd684e1e03e2e87930610c47a351d7a971c4d6deb7916867632635aa81372e048ca2de26ce27',
    'Referer': 'https://scon.stj.jus.br/SCON/pesquisar.jsp',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Mobile Safari/537.36 Edg/109.0.1518.55',
    'sec-ch-ua': '"Not_A Brand";v="99", "Microsoft Edge";v="109", "Chromium";v="109"',
    'sec-ch-ua-mobile': '?1',
    'sec-ch-ua-platform': '"Android"',
}

# {
#     'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
#     'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6,ja;q=0.5',
#     'Cache-Control': 'max-age=0',
#     'Connection': 'keep-alive',
#     # 'Cookie': 'f5_cspm=1234; JSESSIONID=W5hlk1-uDQhzLhR66JN3lNdNV5qDgolZorpGTyty.svlp-jboss-01; TS01603393=016a5b3833b3834c7fed9a309c8228bc1a920557481e443674e7e9316852bbd0daad16b56b1994c1eb74f44023598e2fe5cccd88d2; TS0133e456=0132b05847992e906502f2b8833d7a169f0ebe088934c98bf0763a9f29f7d5f055b1392f930b86f7493b394d6d72339819638f17d6a73b2f8f876a142ff729715452a228556f8da23a47c05c892f35379533d91462; _ga=GA1.3.84998084.1664279080; BIGipServerpool_svlp-jboss_scon=1057204416.36895.0000; BIGipServerpool_wserv=1023413514.20480.0000; _gid=GA1.3.1107844980.1673011176; TS01dc523b=016a5b38336cefb0eb88e7ceabe09af8ec89e1be4026faca030645367db23f7858fa343b8ed3d7a6094e1bb1e6566d976957c0e1e0; TS0122e3df=0132b058476d15365f25ba6d43f5b0af647c796990e69ae6548c91554175157ca85409b9576793e9bbcd3bcfbf4e472eb176fcf58ebc5dc9a4459ce136a3d98ccc16891973; TS0165095f=0132b0584735807162bbe8ab6f7337f5364d90a466f643011574ee91c4c5f4c7f32853929651abf57112855968226788ddc91dc689d976b98f8c8d2924baeb7c6eb0f4258624a25404def5a16d5a8ec9701c0c2cad41d0cccc47f112cc222773368f96dbbb; TS01e2b0fb=016a5b3833850a7a591750d7d5c3df75877543384ac77b4a16aafda6071dbe6533f9059f308778de66bc3b08572f87b5f6d09877c3; TS013f88e9=0132b058470d915587f5f9f285593b226b05e62b3634c98bf0763a9f29f7d5f055b1392f93e1a51c5a394448494de52da6ca4628da3fd96db8f2c611bba389bbdcdd9a0974',
#     'Origin': 'https://scon.stj.jus.br',
#     'Referer': 'https://scon.stj.jus.br/SCON/',
#     'Sec-Fetch-Dest': 'document',
#     'Sec-Fetch-Mode': 'navigate',
#     'Sec-Fetch-Site': 'same-origin',
#     'Sec-Fetch-User': '?1',
#     'Upgrade-Insecure-Requests': '1',
#     'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Mobile Safari/537.36 Edg/108.0.1462.54',
#     'sec-ch-ua': '"Not?A_Brand";v="8", "Chromium";v="108", "Microsoft Edge";v="108"',
#     'sec-ch-ua-mobile': '?1',
#     'sec-ch-ua-platform': '"Windows"',
# }

def get_filters(start_date : pendulum.DateTime, end_date : pendulum.DateTime):
  date_filter = f'@DTPB >= "{start_date.format(DATE_FORMAT_1)}" E @DTPB <= "{end_date.format(DATE_FORMAT_1)}"'
  return {
# 'pesquisaAmigavel':'+%3Cb%3EPublica%E7%E3o%3A+01%2F02%2F2022+a+01%2F03%2F2022%3C%2Fb%3E',
'acao': 'pesquisar',
'novaConsulta': 'true',
# 'i': 1,
'b': 'DTXT',
'livre': '',
'filtroPorOrgao': '',
'filtroPorMinistro': '',
'filtroPorNota': '',
'data': date_filter,
'operador': 'e',
'p': 'true',
'tp': 'T',
'processo': '',
'classe': '',
'uf': '',
'relator': '',
'dtpb': date_filter,
'dtpb1':start_date.format(DATE_FORMAT_2),
'dtpb2':end_date.format(DATE_FORMAT_2),
'dtde': '',
'dtde1': '',
'dtde2': '',
'orgao': '',
'ementa': '',
'nota': '',
'ref': '',
  }

def get_row_filters(start_date : pendulum.DateTime, end_date : pendulum.DateTime):
  date_filter = f'@DTPB >= "{start_date.format(DATE_FORMAT_1)}" E @DTPB <= "{end_date.format(DATE_FORMAT_1)}"'
  return {
    'numDocsPagina': '50',
    'tipo_visualizacao': '',
    'filtroPorNota': '',
    'ref': '',
    'p': 'true',
    'b': 'DTXT',
    'data': date_filter,
    # 'i': '11',
    # 'l': '10',
    'tp': 'T',
    'operador': 'E',
}

class STJMONOClient:

  def __init__(self):
    self.base_url  = 'https://scon.stj.jus.br'
    self.requester = requests.Session()
    self.requester.headers = DEFAULT_HEADERS
    self.requester.get("https://scon.stj.jus.br/SCON/", verify=False)

  def reset_session(self):
    self.requester = requests.Session()
    self.requester.headers = DEFAULT_HEADERS
    self.requester.get("https://scon.stj.jus.br/SCON/", verify=False)# headers=DEFAULT_HEADERS, verify=False)

  @utils.retryable(max_retries=3)
  def count(self, filters):

    params = {
    'data': filters['data'],
    'b': 'DTXT',
    'p': 'true',
    'tp': 'T',
    }
    response = self.requester.get('https://scon.stj.jus.br/SCON/pesquisar.jsp', 
      params=params, headers=DEFAULT_HEADERS_COUNT)
    return self._count_by_content(response.content)

  # @utils.retryable(max_retries=3)
  # def fetch(self, filters, offset):
  #   return self._response_or_retry(data={**filters, 'i': offset})

  # @utils.retryable(max_retries=3)
  # def get(self, path):
  #   return self.requester.get(f'{self.base_url}/{path}', verify=False)

  # @utils.retryable(max_retries=3)
  # def _response_or_retry(self, data):
  #   import random
  #   response = self.requester.post(
  #     f'{self.base_url}/SCON/pesquisar.jsp',
  #     # headers=DEFAULT_HEADERS,
  #     headers={
  #     **fake_headers.headers.make_header(), 
  #     'user-agent':random.choice(fake_useragent.UserAgent(use_external_data=False).data_browsers['chrome'])
  #     },
  #     verify=False,
  #     data=data)
  #   soup = utils.soup_by_content(response.content)

  #   if soup \
  #       .find('div', id='idCaptchaLinha'):
  #     logger.warn('Got captcha -- resetting session.')
  #     self.reset_session()
  #     raise utils.PleaseRetryException()

  #   info = soup.find('span', {'class': 'numDocs'}) or \
  #     soup.find('div', {'class':'erroMensagem'})
  #   if not info:
  #     logger.warn('Got invalid page -- reseting session.')
  #     self.reset_session()
  #     raise utils.PleaseRetryException()

  #   return response

  @utils.retryable(max_retries=3)
  def _response_or_retry_rows(self, data):
    import urllib
    
    response = self.requester.post(
      f'{self.base_url}/SCON/decisoes/toc.jsp',
      headers={
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6,ja;q=0.5',
    'Cache-Control': 'max-age=0',
    'Connection': 'keep-alive',
    # 'Cookie': 'JSESSIONID=8kQY3aXD1VPZ5IVbhLjIEDcHd5PB4DuimS_C69Rg.svlp-jboss-01; f5_cspm=1234; TS01603393=016a5b3833a15eb015ace80e186f683e0d04467b8d64f06c40cd51985b9f8c6328151b43f458c077e6f6ff4b42127f5e2d062dbaf7; TS0133e456=0132b058479e24edeb017fcd61d7a6c40e0d80f09d0b0876d8998feb90b4841f679376c6779983dbc5ac7dcc726240293274a69a0929373762048a9354f05df6fe2a716a787abe46efb6a8b8ad448ff82d36fd4d76; _ga=GA1.3.84998084.1664279080; _gid=GA1.3.209578465.1674152937; BIGipServerpool_svlp-jboss_scon=1057204416.36895.0000; TS0165095f=0132b0584708a20341834675af853e92538767377a0b0876d8998feb90b4841f679376c677cb0899e28c8335cd81589077f5f3da12090509f59a011eefd3fc1050ec79288776a5df90d6b154f179a90cf6c0e4fec7; _gat_UA-179972319-1=1; TS01e2b0fb=016a5b3833b24ce57067e8b8fa8a67836ad4c20f80da9a4586fa50b6bbcbcceaafaa405cb659531f52fed09424cf514050ebf8cc46; TS013f88e9=0132b05847169820d2b92a82b979afde26ce9a0da20b0876d8998feb90b4841f679376c6771aa4b8338fb5bcd56faeb445b13eccf6181e1e23f590bcbab5606b4c5d5cbd9c',
    'Origin': 'https://scon.stj.jus.br',
    'Referer': f'https://scon.stj.jus.br/SCON/pesquisar.jsp?data={urllib.parse.quote_plus(data["data"])}&b=DTXT&p=true&tp=T',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Mobile Safari/537.36 Edg/109.0.1518.55',
    'sec-ch-ua': '"Not_A Brand";v="99", "Microsoft Edge";v="109", "Chromium";v="109"',
    'sec-ch-ua-mobile': '?1',
    'sec-ch-ua-platform': '"Android"',
 },
#  {
#       **fake_headers.headers.make_header(), 
#       'user-agent':random.choice(fake_useragent.UserAgent(use_external_data=False).data_browsers['chrome'])
#       },
      verify=False,
      data=data)
    soup = utils.soup_by_content(response.content)

    if soup \
        .find('div', id='idCaptchaLinha'):
      logger.warn('Got captcha -- reseting session.')
      self.reset_session()
      raise utils.PleaseRetryException()

    info = soup.find('span', {'class': 'numDocs'}) or \
      soup.find('div', {'class':'erroMensagem'})
    if not info:
      logger.warn('Got invalid page -- reseting session.')
      self.reset_session()
      raise utils.PleaseRetryException()

    return response


  @utils.retryable(max_retries=3)
  def fetch_rows(self, filters, offset):
    return self._response_or_retry_rows(data={**filters, 'i': offset})

  def _count_by_content(self, content):
    soup = utils.soup_by_content(content)
    info = soup.find('span', text=re.compile(r'.*monocrátic.*'))
    assert not soup.find('div', {'class':'erroMensagem'})
    assert info
    return int(utils.extract_digits(info.text))

class STJMONOCollector(base.ICollector):

  def __init__(self, client : STJMONOClient, query : dict, **options):
    self.client  = client
    self.query   = query
    self.options = (options or {})

  def count(self) -> int:
    return self.client.count(get_filters(
      self.query['start_date'], self.query['end_date']))

  def chunks(self):
    ranges = list(utils.timely(
      self.query['start_date'], self.query['end_date'], unit='days', step=1))

    for start_date, end_date in reversed(ranges):
      filters = get_row_filters(start_date, end_date)
      count   = self.client.count(filters)

      docs_per_page = 50
      for offset in range(0, count + 1, docs_per_page):
        offset = docs_per_page if not offset else offset + 1
        keys =\
          {'start_date' : start_date.to_date_string(),
            'end_date'  : end_date.to_date_string(),
            'offset'    : offset if offset else docs_per_page,
            'limit'     : count + 1}

        import time
        time.sleep(1.5)
        yield STJMONOChunk(keys=keys,
          client=self.client,
          filters=filters,
          docs_per_page=docs_per_page,
          limit=count + 1,
          prefix=f'{start_date.year}/{start_date.month:02d}/')


class STJMONOChunk(base.Chunk):

  def __init__(self, keys, client, filters, docs_per_page, limit, prefix):
    super(STJMONOChunk, self).__init__(keys, prefix)
    self.client  = client
    self.filters = filters
    self.docs_per_page   = docs_per_page
    self.limit   = limit


  @utils.retryable(max_retries=3)
  def rows(self):

    response = self.client.fetch_rows({
      **self.filters, **{'l': self.docs_per_page, 'numDocsPagina': self.docs_per_page}}, 
      offset=self.keys['offset'])
    
    soup  = utils.soup_by_content(response.content)
    count = self.client._count_by_content(response.content)
    if count == 0:
      return []
    for content in self.page_contents(soup):
      time.sleep(random.random()/2)
      yield content

  def page_contents(self, soup):
    

    @utils.retryable(max_retries=3)
    def _get_pdf_urls(doc):
      BASE_PDF_URL = "https://processo.stj.jus.br"
      a = doc.find('a',attrs={'title':'Decisão Monocrática Certificada'}) or \
        doc.find('a',attrs={'original-title':'Decisão Monocrática Certificada'})
      session = requests.Session()
      pdfs_page = utils.get_response(
        logger, 
        session, 
        utils.find_between(a['href'], start="'", end="'"),
        {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6,ja;q=0.5',
    'Cache-Control': 'max-age=0',
    'Connection': 'keep-alive',
    # 'Cookie': 'JSESSIONID=Ybtm-imnPIJeqZiGx9uh-rDllNlqWJyqbdzaTOLK.svlp-jboss-04; TS01db5b07=0132b058475ed42f31b520f66a8df07be945aea7401e0a42f50d7edc3e19455603d94a47f51e029adcea4130484817e5c6993ad1c7c8589bf12863cdb87e429a4db6b99238; _ga=GA1.3.84998084.1664279080; _gid=GA1.3.209578465.1674152937; BIGipServerpool_svlp-jboss=1107536064.36895.0000; TS01dc523b=016a5b38332c4c19b057799de99ca5f3bd713012bd08976a0b9c2abb671b433e8016f7d554cba1585cb21a6878c2d3bd44837db784; TS0122e3df=0132b05847a79700aa622a420bfddca45fd3c10474043731851d155b40f5db36ac99e383b06265acd5e5d5665032eab031c2cd1ee87f84357b7e607e7e32d8f09e91e33bc2; TS01bf01ec=0132b05847d0b78c9f25e170ecf4f14cd87d405d0c8e55c9ad9142f5a25a562f7d5da8ef33c1524c39f907efcd487606600a020d0a57c76c5f37f345c5e6875d7598a6e173; _gat_UA-179972319-1=1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Mobile Safari/537.36 Edg/109.0.1518.55',
    'sec-ch-ua': '"Not_A Brand";v="99", "Microsoft Edge";v="109", "Chromium";v="109"',
    'sec-ch-ua-mobile': '?1',
    'sec-ch-ua-platform': '"Android"',
    }, 
        verify=False
      )
      a_s = utils.soup_by_content(pdfs_page.text).find_all('a', text='Decisão Monocrática')
      pdf_links = [BASE_PDF_URL + utils.find_between(a['href'], start="'", end="'") for a in a_s]
      return pdf_links


    @utils.retryable(max_retries=3)
    def get_direct_links(pdf_links: List) -> List:
      direct_links = []
      for pdf_link in pdf_links:
        page = utils.get_response(logger, requests.Session(), pdf_link, '', verify=False)
        soup = utils.soup_by_content(page.content)
        link = soup.find('iframe')['src']
        if 'http' not in soup.find('iframe')['src']:
          link = f'https://processo.stj.jus.br{link}'
        direct_links.append(link)
      return direct_links

    def append_metadata(doc, doc_count):
      new_tag=bs4.Tag(name='div', attrs={'class':'expected-count'})
      new_tag.append(f'Number of expected documents: {doc_count}')
      doc.insert(1, new_tag)
      return doc

    docs = soup.find_all(class_='documento')

    for doc in docs:
      pdf_urls = _get_pdf_urls(doc)
      expected_doc_count = len(pdf_urls)

      to_download = []
      
      if expected_doc_count == 0:
        logger.warn(f'No document found. {str(doc)[:400]}')
        continue     
      
      #Checks if all num_registros are equal - should be.
      try:
        assert all(
          utils.get_param_from_url(pdf_urls[0], 'num_registro') == utils.get_param_from_url(url, 'num_registro') 
          for url in pdf_urls)
      except AssertionError:
        logger.error(f'Found links with different num_registro. Skipped. {pdf_urls=}')
        continue

      #Número processo
      act_id  = utils.get_param_from_url(pdf_urls[0], 'num_registro')
      
      #Seq
      seq = utils.get_param_from_url(pdf_urls[0], 'sequencial')
      
      #Date
      publication_date = utils.get_param_from_url(pdf_urls[0], 'data')
      date_obj = pendulum.from_format(publication_date, 'YYYYMMDD')
      day, month, year = date_obj.format('DD'), date_obj.format('MM'), date_obj.format('YYYY')

      #Componente
      componente = utils.get_param_from_url(pdf_urls[0], 'componente')
      if componente != "MON":
        logger.warn(f'Componente is not MON: {act_id=} {componente=} {seq=}')
      
      #Meta Hash
      meta_hash = utils.get_content_hash(doc, [{'name':'p'}])
      
      #Make filename
      filename = f'{year}/{month}/{day}_{componente}_{act_id}_{meta_hash}'

      time.sleep(0.3)

      #Get PDF data
      if expected_doc_count > 1:
        logger.warn(f'Found {expected_doc_count} documents, expected 1. {act_id=}')
        for n, pdf_url in enumerate(get_direct_links(pdf_urls), start=1):
          to_download.append(base.ContentFromURL(
            src=pdf_url, dest=f'{filename}_{n:02}.pdf', content_type='application/pdf')
            )
            
      elif expected_doc_count == 1:
        to_download.append(base.ContentFromURL(
          src=get_direct_links(pdf_urls).pop(), dest=f'{filename}.pdf', content_type='application/pdf')
          )

    #Get Metadata
      to_download.append(
            base.Content(
              content=append_metadata(doc, expected_doc_count).prettify(), 
              dest=f'{filename}.html', 
              content_type='text/html'))
      
      yield to_download

@celery.task(queue='crawlers.stjmono', default_retry_delay=5 * 60,
             autoretry_for=(BaseException,))
def stjmono_task(start_date, end_date, output_uri):
  setup_cloud_logger(logger)
 
  from app.crawlers.logutils import logging_context
  
  with logging_context(crawler='STJMONO'):
    output = utils.get_output_strategy_by_path(path=output_uri)
    logger.info(f'Output: {output}.')

    start_date, end_date =\
      pendulum.parse(start_date), pendulum.parse(end_date)

    query_params = {'start_date': start_date, 'end_date': end_date}
    collector = STJMONOCollector(client=STJMONOClient(), query=query_params)
    handler   = base.ContentHandler(output=output)

    snapshot = base.Snapshot(keys=query_params)
    base.get_default_runner(
        collector=collector, output=output, handler=handler, logger=logger, max_workers=8) \
      .run(snapshot=snapshot)


@cli.command(name='stjmono')
@click.option('--start-date',
  default=utils.DefaultDates.THREE_MONTHS_BACK.strftime("%Y-%m-%d"),
  help='Format YYYY-MM-DD.',
)
@click.option('--end-date'  ,
  default=utils.DefaultDates.NOW.strftime("%Y-%m-%d"),
  help='Format YYYY-MM-DD.',
)
@click.option('--output-uri', default=None,  help='Output URI (e.g. gs://bucket_name')
@click.option('--enqueue'   , default=False, help='Enqueue for a worker'  , is_flag=True)
@click.option('--split-tasks',
  default=None, help='Split tasks based on time range (weeks, months, days, etc) (use with --enqueue)')
def stjmono_command(start_date, end_date, output_uri, enqueue, split_tasks):
  args = (start_date, end_date, output_uri)
  if enqueue:
    if split_tasks:
      start_date, end_date =\
        pendulum.parse(start_date), pendulum.parse(end_date)
      for start, end in reversed(list(utils.timely(start_date, end_date, unit=split_tasks, step=1))):
        task_id = stjmono_task.delay(
          start.to_date_string(),
          end.to_date_string(),
          output_uri)
        print(f"task {task_id} sent with params {start.to_date_string()} {end.to_date_string()}")
    else:
      stjmono_task.delay(*args)
  else:
    stjmono_task(*args)