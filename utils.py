import os
import json
import logging
import urllib.request
import time
import http.client
import requests
import itertools
import hashlib
import io
from tqdm import tqdm
from datetime import datetime
from pathlib import Path
from bs4 import BeautifulSoup
from google.cloud import storage
from functools import wraps
import wrapt
from urllib.parse import urlencode

from storage import (get_bucket_ref, )
from fake_useragent import UserAgent


class GSOutput:
    def __init__(self, bucket_name):
        self._bucket_name = bucket_name
        self._bucket = get_bucket_ref(bucket_name)

    def exists(self, filepath):
        blob = self._bucket.blob(filepath)
        return blob.exists()

    def save_from_contents(self, filepath, contents, **kwargs):
        blob = self._bucket.blob(filepath)
        blob.upload_from_string(contents,
            content_type=kwargs.get('content_type', 'text/plain'))

    def load_as_string(self, filepath):
        blob = self._bucket.blob(filepath)
        if blob.exists():
            return blob.download_as_bytes()

    @property
    def uri(self):
        return f'gs://{self._bucket_name}'

    def __repr__(self):
        return f"GS({self._bucket})"


class LSOutput:
    def __init__(self, output_folder):
        self._output_folder = output_folder

    def exists(self, filepath):
        source = f'{self._output_folder}/{filepath}'
        return Path(source).exists()

    def save_from_contents(self, filepath, contents, **kwargs):
        target = f'{self._output_folder}/{filepath}'
        write_file(target, contents, mode=kwargs.get('mode', 'w'))

    def load_as_string(self, filepath):
        source = f'{self._output_folder}/{filepath}'
        if Path(source).exists():
            with open(source, 'r') as f:
                return f.read()

    @property
    def uri(self):
        return self._output_folder

    def __repr__(self):
        return f"Local({self._output_folder})"


def get_output_strategy_by_path(path):
    from urllib.parse import urlparse
    url = urlparse(path)
    if url.scheme == 'gs':
        return GSOutput(bucket_name=url.netloc)
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


def write_file(filename, content, mode='a'):
    path = os.path.dirname(filename)
    Path(path).mkdir(parents=True, exist_ok=True)
    file = open(filename, mode)
    file.write(content)
    file.close()


def soup_by_content(content):
    return BeautifulSoup(content, features='html.parser')


def get_filepath(date, filename, extension):
    _, month, year = date.split('/')
    return f'{year}/{month}/{filename}.{extension}'


def pdf_content_file_by_url(pdf_url):
    response = urllib.request.urlopen(pdf_url)
    if response.code == 200 and response.headers['Content-Type'] == 'application/pdf':
        return response.read()
    else:
        raise Exception(
            f'Got {response.status_code} fetching {pdf_url} -- expected `200`.')


class PleaseRetryException(Exception):
    pass


def retryable(*, max_retries=3, sleeptime=5,
              retryable_exceptions=(
                  requests.exceptions.ConnectionError,
                  requests.exceptions.ReadTimeout,
                  http.client.HTTPException,
                  PleaseRetryException)):
    assert max_retries > 0 and sleeptime > 1

    @wrapt.decorator
    def wrapper(wrapped, instance, args, kwargs):
        retry_count = 0
        while retry_count < max_retries:
            try:
                return wrapped(*args, **kwargs)
            except retryable_exceptions as ex:
                retry_count = retry_count + 1
                if retry_count == max_retries:
                    instance.logger.fatal(
                        f'Retry count exceeded (>{max_retries})')
                    raise ex
                instance.logger.warn(
                    f'Got connection issues -- retrying in {retry_count * 5}s.')
                time.sleep(sleeptime * retry_count)

    return wrapper


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

    def __init__(self,logger,level=None):
        super(TqdmToLogger, self).__init__()
        self.logger = logger
        self.level = level or logging.INFO

    def write(self,buf):
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
        yield dates[0], dates[0]
    else:
        if end_date > dates[-1]:
            dates.append(end_date)
        pairs = list(pairwise(dates))
        for x, y in pairs[:-1]:
            yield x, y.subtract(days=1)
        yield pairs[-1]


class Chunk:
  def __init__(self, params, output, rows_generator):
    self._params = params
    self._output = output
    self._rows_generator = rows_generator
    self._values = {**{}, **params}
    if self.commited():
      self._restore_values()

  def _restore_values(self):
    raw_value = self._output.load_as_string(self.filepath)
    self._values = json.loads(raw_value)

  @property
  def filepath(self):
    return f'.state/{self.hash}.state'

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