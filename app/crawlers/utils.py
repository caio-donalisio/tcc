import bs4
import concurrent.futures
import hashlib
import http.client
import io
import itertools
import json
import logging
import os
import pendulum
import re
import requests
import time
import urllib.request
import wrapt
import random

from fake_useragent import UserAgent
from app.crawlers import base, headers
from app.crawlers.logconfig import logger
from app.crawlers.storage import get_bucket_ref

from bs4 import BeautifulSoup
from datetime import datetime
from fake_useragent import UserAgent
from functools import partial
from functools import wraps
from itertools import chain
from pathlib import Path
from selenium.common.exceptions import TimeoutException
from tqdm import tqdm
from typing import Callable, List, Dict
from urllib.parse import parse_qsl, urlsplit

CURRENT_TIME = pendulum.now()


class DefaultDates:
  NOW = CURRENT_TIME
  TWO_YEARS_BACK = CURRENT_TIME.subtract(days=365 * 2)
  BEGINNING_OF_YEAR_OR_SIX_MONTHS_BACK = min(pendulum.datetime(
      year=CURRENT_TIME.year, month=1, day=1), CURRENT_TIME.subtract(days=30 * 6))
  THREE_MONTHS_BACK = CURRENT_TIME.subtract(days=30 * 3)
  TWO_MONTHS_BACK = CURRENT_TIME.subtract(days=30 * 2)
  BEGINNING_OF_MONTH = CURRENT_TIME.set(day=1)
  LAST_YEAR = CURRENT_TIME.subtract(days=365).set(month=1, day=1)
  LAST_WEEK = CURRENT_TIME.subtract(days=7)
  BEGINNING_OF_MONTH_OR_LAST_WEEK = min(
      CURRENT_TIME.subtract(days=7), CURRENT_TIME.set(day=1))


class PleaseRetryException(Exception):
  pass


def retryable(*, max_retries=3, sleeptime=5,
              message='Got connection issues',
              ignore_if_exceeds=False,
              retryable_exceptions=(
                  requests.exceptions.ConnectionError,
                  requests.exceptions.ReadTimeout,
                  requests.exceptions.ChunkedEncodingError,
                  http.client.HTTPException,
                  TimeoutException,
                  PleaseRetryException)):
  assert max_retries > 0 and sleeptime > 0

  @wrapt.decorator
  def wrapper(wrapped, instance, args, kwargs):
    retry_count = 0
    while retry_count < max_retries:
      try:
        val = wrapped(*args, **kwargs)
        if retry_count > 0:
          logger.info(f'Succeed after {retry_count} retries.')
        return val
      except retryable_exceptions as ex:
        retry_count = retry_count + 1
        if retry_count > max_retries:
          logger.fatal(
              f'Retry count exceeded (>{max_retries})')
          raise ex
        logger.warn(ex)
        logger.warn(
            f'{message} -- retrying in {retry_count * sleeptime}s.')
        time.sleep(sleeptime * retry_count)
    if not ignore_if_exceeds:
      raise Exception(f'Retry count exceeded (>{max_retries})')

  return wrapper


class GSOutput:
  def __init__(self, bucket_name, prefix=''):
    self._bucket_name = bucket_name
    self._bucket = get_bucket_ref(bucket_name)
    self._prefix = prefix
    self._cache = {}
    # self._cache  = {b.name: True for b in self.list_by_prefix()}

  @retryable(max_retries=5, message='Could not connect to GCP')
  def list_by_prefix(self, prefix=None):
    return list(self._bucket.list_blobs(prefix=prefix))

  @retryable(max_retries=5, message='Could not connect to GCP')
  def exists(self, filepath):
    if self._cache.get(f'{self._prefix}{filepath}'):
      return True
    blob = self._bucket.blob(f'{self._prefix}{filepath}')
    return blob.exists()

  @retryable(max_retries=5, message='Could not connect to GCP')
  def save_from_contents(self, filepath, contents, **kwargs):
    blob = self._bucket.blob(f'{self._prefix}{filepath}')
    blob.upload_from_string(contents,
                            content_type=kwargs.get('content_type', 'text/plain'))

  @retryable(max_retries=5, message='Could not connect to GCP')
  def load_as_string(self, filepath):
    blob = self._bucket.blob(f'{self._prefix}{filepath}')
    if blob.exists():
      return blob.download_as_bytes()

  @property
  def uri(self):
    return f'gs://{self._bucket_name}{self._prefix}'

  def __repr__(self):
    return f"GS({self._bucket}, prefix={self._prefix})"


class LSOutput:
  def __init__(self, output_folder):
    self._bucket_name = output_folder

  def exists(self, filepath):
    source = f'{self._bucket_name}/{filepath}'
    return Path(source).exists()

  def save_from_contents(self, filepath, contents, **kwargs):
    target = f'{self._bucket_name}/{filepath}'
    write_file(target, contents)

  def load_as_string(self, filepath):
    source = f'{self._bucket_name}/{filepath}'
    if Path(source).exists():
      with open(source, 'r') as f:
        return f.read()

  @property
  def uri(self):
    return self._bucket_name

  def __repr__(self):
    return f"Local({self._bucket_name})"


def get_output_strategy_by_path(path):
  from urllib.parse import urlparse
  url = urlparse(path)
  if url.scheme == 'gs':
    return GSOutput(bucket_name=url.netloc, prefix=url.path[1:])
  elif url.scheme == '':  # assume as local filesystem.
    return LSOutput(output_folder=path)
  raise Exception(f'Unable to detect proper output strategy for {path}')


def get_daterange(args):
  return '-'.join([args.start_date.replace('/', ''),
                   args.end_date.replace('/', '')])


def setup_logger(name, log_file, level=logging.INFO):
  Path(log_file).parent.mkdir(parents=True, exist_ok=True)
  logger = logging.getLogger(name)
  logging.basicConfig(
      filename=log_file,
      format='[%(asctime)s] %(levelname)s: %(message)s',
      level=level
  )
  logging.getLogger('urllib3').setLevel(logging.WARNING)
  return logger


def now():
  return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def write_file(filename, content):
  path = os.path.dirname(filename)
  Path(path).mkdir(parents=True, exist_ok=True)
  mode = 'w'
  if isinstance(content, bytes):
    mode = 'wb'
  elif isinstance(content, str):
    mode = 'w'
  with open(filename, mode) as file:
    file.write(content)


def convert_doc_to_pdf(bytes, container_url):
  """Returns PDF Bytes, converted by the unoconv container"""
  # zrrrzzt/docker-unoconv-webservice
  try:
    response = requests.post(container_url, files={'file': bytes})
  except requests.exceptions.ConnectionError:
    logger.warn('doc-to-pdf container not available ')
    raise PleaseRetryException()
  if response.status_code == 200:
    return response.content
  else:
    logger.warn('Could not convert doc to pdf')


def soup_by_content(content):
  return BeautifulSoup(content, features='html.parser')


def get_filepath(date, filename, extension):
  _, month, year = date.split('/')
  return f'{year}/{month}/{filename}.{extension}'


def extract_digits(text: str):
  """Extracts digits only from a given string

  Args:
      text (str): Input text

  Returns:
      str: String containing digits only
  """

  import re

  return re.sub(r'[^\d]', '',  text)


def get_content_hash(
        soup: bs4.BeautifulSoup,
        tag_descriptions: List[Dict],
        length=10):
  '''
  Return a hash representing the content of the given tags within a soup
  '''
  content_string = ''.join(
      tag.text
      for tag in chain.from_iterable(
          soup.find_all(**tag_description) for tag_description in tag_descriptions
      )
  )
  return hashlib.sha1(content_string.encode('utf-8')).hexdigest()[:length]


def get_count_filepath(
        court_name: str,
        start_date: pendulum.datetime,
        end_date: pendulum.datetime,
        filepath=None):
  '''
  Returns custom or standard count file name
  '''
  if filepath is None:
    dest_record = f'{court_name.upper()}-COUNT-{start_date.to_date_string()}-{end_date.to_date_string()}.json'
  else:
    dest_record = filepath
  return dest_record


def get_count_data(
        start_date: pendulum.datetime,
        end_date: pendulum.datetime,
        count: int,
        count_time: pendulum.datetime,):
  '''
  Returns count data dictionary
  '''
  return {
      'start_date': start_date.to_date_string(),
      'end_date': end_date.to_date_string(),
      'count': count,
      'count_time': count_time.to_datetime_string(),
  }


def get_count_data_and_filepath(
        court_name: str,
        start_date: pendulum.datetime,
        end_date: pendulum.datetime,
        count: int,
        count_time: pendulum.datetime,
        filepath=None):

  return tuple([
      get_count_data(start_date, end_date, count, count_time),
      get_count_filepath(court_name, start_date, end_date, filepath)
  ])


def count_data_content(count_data, count_filepath):
  return [
      base.Content(
          content=json.dumps(count_data),
          dest=count_filepath,
          content_type='application/json'
      )
  ]


def pdf_content_file_by_url(pdf_url):
  response = urllib.request.urlopen(pdf_url)
  if response.code == 200 and response.headers['Content-Type'] == 'application/pdf':
    return response.read()

  raise Exception(
      f'Got {response.status_code} fetching {pdf_url} -- expected `200`.'
  )


def try_multiple_encodings(possible_encodings=['utf-8', 'latin-1', 'ISO-8859-9', 'ISO-8859-1']):
  for encoding in possible_encodings:
    def outter(func):
      def inner(*args, encoding=encoding, **kwargs):
        try:
          return func(*args, encoding=encoding, **kwargs)
        except UnicodeEncodeError:
          pass
        except Exception as e:
          raise PleaseRetryException()
      return inner
    return outter


@try_multiple_encodings()
def get_pdf_hash(pdf_content: bytes,
                 encoding: str,
                 remove_whitespace=True,
                 length=10):

  from io import BytesIO
  import PyPDF2

  try:
    pdf_content = PyPDF2.PdfReader(BytesIO(pdf_content))
    pdf_content = ''.join(pdf_content.getPage(i).extract_text()
                          for i in range(pdf_content._get_num_pages()))
    if remove_whitespace:
      pdf_content = pdf_content.replace(' ', '')
      pdf_hash = hashlib.sha1(pdf_content.encode(
          encoding)).hexdigest()[:length]
  except TypeError:
    pdf_hash = '0' * length
  return pdf_hash


@retryable(max_retries=9)
def get_response(logger, session, url, headers, verify=True):
  """Gets response and checks if response object has status code 200, throws Retry exception if not"""
  response = session.get(
      url=url, headers=headers, verify=verify)
  if response.status_code != 200:
      logger.warn(f"Response <{response.status_code}> - {response.url}")
      raise PleaseRetryException()
  else:
    return response


def get_soup_xpath(element):
  """Returns the XPATH for a given bs4 element"""
  components = []
  child = element if element.name else element.parent
  for parent in child.parents:
    siblings = parent.find_all(child.name, recursive=False)
    components.append(
        child.name if 1 == len(siblings) else '%s[%d]' % (
            child.name,
            next(i for i, s in enumerate(siblings, 1) if s is child)
        )
    )
    child = parent
  components.reverse()
  return '/%s' % '/'.join(components)


class TqdmToLogger(io.StringIO):
  logger = None
  level = None
  buf = ''

  def __init__(self, logger, level=None):
    super(TqdmToLogger, self).__init__()
    self.logger = logger
    self.level = level or logging.INFO

  def write(self, buf):
    self.buf = buf.strip('\r\n\t ')

  def flush(self):
    self.logger.log(self.level, self.buf)


def pairwise(iterable):
  a, b = itertools.tee(iterable)
  next(b, None)
  return zip(a, b)


def monthly(start_date, end_date):
  date_range = start_date.diff(end_date)
  if date_range.in_months() > 0:
    yield from timely(start_date, end_date, unit='months', step=1)
  else:
    yield start_date, end_date


def weekly(start_date, end_date):
  date_range = start_date.diff(end_date)
  if date_range.in_weeks() > 0:
    yield from timely(start_date, end_date, unit='weeks', step=1)
  else:
    yield start_date, end_date


def timely(start_date, end_date, unit, step):
  date_range = start_date.diff(end_date)
  dates = list(date_range.range(unit, step))
  if len(dates) == 1:
    yield start_date, end_date
  else:
    if end_date >= dates[-1]:
      dates.append(end_date)
    pairs = list(pairwise(dates))
    for x, y in pairs[:-1]:
      yield x, y.subtract(days=1)
    yield pairs[-1]


class Chunk:
  def __init__(self, params, output, rows_generator, prefix=''):
    self._params = params
    self._output = output
    self._prefix = prefix
    self._rows_generator = rows_generator
    self._values = {**{}, **params}
    if self.commited():
      self._restore_values()

  def _restore_values(self):
    raw_value = self._output.load_as_string(self.filepath)
    self._values = json.loads(raw_value)

  @property
  def params(self):
    return self._params

  @property
  def filepath(self):
    return f'.state/{self._prefix}{self.hash}.state'

  def commit(self):
    self._output.save_from_contents(
        filepath=self.filepath, contents=json.dumps(self._values),
        content_type='application/json')

  def commited(self):
    return self._output.exists(self.filepath)

  def set_value(self, key, value):
    self._values[key] = value

  def get_value(self, key):
    return self._values[key]

  @property
  def hash(self):
    return hashlib.sha1(repr(sorted(self._params.items())).encode()) \
        .hexdigest()

  def rows(self):
    yield from self._rows_generator


def generate_headers(origin=None, accept='*/*', user_agent=None, xhr=False):
  headers = {
      'Accept': accept,
      'User-Agent': (user_agent or 'Mozilla/5.0 (X11; Linux x86_64; rv:53.0.2) Gecko/20100101 Firefox/53.0.2'),
      'Referer': 'https://www.google.com/',
  }
  if origin:
    headers['Origin'] = origin
  if xhr:
    headers['X-Requested-With'] = 'XMLHttpRequest'
  return headers


class HeaderGenerator:
  def __init__(self, **defaults):
    self.ua = UserAgent()
    self.defaults = defaults

  def generate(self):
    return generate_headers(
        accept=self.defaults.get('accept'),
        origin=self.defaults.get('origin'),
        xhr=self.defaults.get('xhr', False),
    )


def timeit(f):
  def timed(*args, **kw):
    ts = time.time()
    result = f(*args, **kw)
    te = time.time()
    print('func:%r took: %2.4f sec' % (f.__name__, te-ts))
    return result
  return timed


class CoolDownDecorator(object):
  def __init__(self, func, interval):
    self.func = func
    self.interval = interval
    self.last_run = 0

  def __get__(self, obj, objtype=None):
    if obj is None:
      return self.func
    return partial(self, obj)

  def __call__(self, *args, **kwargs):
    now = time.time()
    if now - self.last_run < self.interval:
      time.sleep(now - self.last_run)
      return self.func(*args, **kwargs)
    else:
      self.last_run = now
      return self.func(*args, **kwargs)


def cooldown(interval):
  def decorated(func):
    decorator = CoolDownDecorator(func=func, interval=interval)
    return wraps(func)(decorator)
  return decorated


def get_param_from_url(url, param):
  try:
    query = urlsplit(url).query
    value = dict(parse_qsl(query))[param]
    return value
  except Exception as e:
    print("url", url, param)
    raise e


def find_between(string, start, end):
  pattern = f"{start}(.*?){end}"
  return re.search(pattern, string).group(1)


def run_pending_tasks(task: Callable, pendings: list, max_workers: int = 4, **kwargs):
  dry_run = bool(kwargs.get('dry_run', False))
  input_uri = kwargs.get('input_uri', None)

  batch = []
  with tqdm(total=len(pendings)) as pbar:
    executor = concurrent.futures.ThreadPoolExecutor(
        max_workers=max_workers)
    futures = []
    for pending in pendings:
      if dry_run:
        continue

      batch.append(pending)
      if len(batch) >= 5:
        futures.append(executor.submit(task, batch, input_uri, pbar))
        batch = []

    print("Tasks distributed -- waiting for results")
    for future in concurrent.futures.as_completed(futures):
      future.result()
    executor.shutdown()
    if len(batch):
      task(batch, input_uri, pbar)


def get_random_useragent():
  try:
    # raise Exception
    return random.choice(random.choice(list(UserAgent().data_browsers.values())))
  except Exception:
    return random.choice(headers.POSSIBLE_USER_AGENTS)

