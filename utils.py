import os
import logging
import urllib.request
import time
import http.client
import requests
import io
from tqdm import tqdm
from datetime import datetime
from pathlib import Path
from bs4 import BeautifulSoup
from google.cloud import storage
from functools import wraps
import wrapt
from urllib.parse import urlencode


class GSOutput:
    def __init__(self, bucket_name, client=None):
        self._bucket_name = bucket_name
        self._client = (client or storage.Client())
        self._bucket = self._client.get_bucket(bucket_name)

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