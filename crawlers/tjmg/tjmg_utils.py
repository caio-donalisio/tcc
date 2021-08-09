import os
import logging
import urllib.request
import time
import http.client
import requests
import argparse
import wrapt
from datetime import datetime
from pathlib import Path
from bs4 import BeautifulSoup
from google.cloud import storage
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from urllib.parse import urlencode


class GSOutput:
    def __init__(self, bucket_name, client=None):
        self._client = (client or storage.Client())
        self._bucket = self._client.get_bucket(bucket_name)

    def save_from_contents(self, filepath, contents, **kwargs):
        blob = self._bucket.blob(filepath)
        blob.upload_from_string(contents)

    def load_as_string(self, filepath):
        blob = self._bucket.blob(filepath)
        if blob.exists():
            return blob.download_as_bytes()


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


class FirefoxBrowser:
    def __init__(self, headers=None, headless=True):
        options = self._get_options(headers, headless)
        self.driver = webdriver.Firefox(options=options)

    def get(self, url, wait_for=(By.TAG_NAME, 'body')):
        self.driver.get(url)
        if wait_for is not None:
            self.wait_for_element(locator=wait_for)

    def close_current_window(self):
        self.driver.close()

    def quit(self):
        self.driver.quit()

    def current_url(self):
        return self.driver.current_url

    def page_source(self):
        return self.driver.page_source

    def click(self, element, wait_for=(By.TAG_NAME, 'body')):
        xpath = get_soup_xpath(element)
        self.driver.find_element(By.XPATH, xpath).click()
        if wait_for is not None:
            self.wait_for_element(locator=wait_for)

    def switch_to_window(self, index):
        window_handle = self.driver.window_handles[index]
        self.driver.switch_to.window(window_handle)

    def wait_for_element(self, locator, timeout=10, frequency=0.5):
        wait = WebDriverWait(self.driver, timeout, poll_frequency=frequency)
        element = wait.until(EC.element_to_be_clickable(locator))
        return element

    def back(self):
        self.driver.execute_script('window.history.go(-1)')

    def select_option(self, bs4_element, option_text):
        xpath = get_soup_xpath(bs4_element)
        select = Select(self.driver.find_element_by_xpath(xpath))
        if option_text:
            select.select_by_visible_text(option_text)

    def fill_in(self, field_id, value):
        start_input = self.driver.find_element_by_id(field_id)
    def fill_in(self, selector, value):
        html_property = selector[1:]
        if selector[0] == '#':
            start_input = self.driver.find_element_by_id(html_property)
        else:
            start_input = self.driver.find_element_by_class_name(html_property)
        start_input.clear()
        start_input.send_keys(value)

    def is_text_present(self, substring, tag="*"):
        try:
            xpath = f"//{tag}[contains(text(),'{substring}')]"
            self.driver.find_element_by_xpath(xpath)
            return True
        except NoSuchElementException:
            return False

    def get_cookie(self, cookie_name):
        value = None
        cookies = self.driver.get_cookies()
        for cookie in cookies:
            if cookie.get('name') == cookie_name:
                value = cookie['value']
        if cookie is None:
            raise Exception(f'Cookie not found: {cookie_name}')
        return value

    def _get_options(self, headers, headless):
        options = Options()
        headers = urlencode(headers or self._sample_headers())
        options.add_argument(headers)
        if headless:
            options.add_argument('--headless')
        return options

    def _sample_headers(self):
        return {'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) \
                            AppleWebKit/537.36 (KHTML, like Gecko) \
                            Chrome/91.0.4472.114 Safari/537.36'}


class BSCrawler:
    def __init__(self, output, logger, query=None, requester=None, browser=None, state_filename=''):
        self.output = output
        self.logger = logger
        self.query = query
        self.state_filename = state_filename
        self.browser = browser
        self.requester = requester

    def _current_soup(self):
        return soup_by_content(self.browser.page_source())

    def _find(self, matcher=None, **kwargs):
        return self._current_soup().find(matcher, **kwargs)

    def _find_all(self, matcher=None, **kwargs):
        return self._current_soup().find_all(matcher, **kwargs)


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
            f'Got {response.code} fetching {pdf_url} -- expected `200`.')


def retryable(*, max_retries=3, sleeptime=5,
              retryable_exceptions=(
                  requests.exceptions.ConnectionError,
                  requests.exceptions.ReadTimeout,
                  http.client.HTTPException)):
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


def default_argument_parser(add_range=True, add_headless=True):
    parser = argparse.ArgumentParser()
    parser.add_argument('--bucket', type=str, help='Bucket name', default=None)
    if add_range:
        parser.add_argument('start_date', type=str,
                            help='Start publication date')
        parser.add_argument('end_date', type=str, help='End publication date')
    if add_headless:
        parser.add_argument('--headless', type=int, nargs='?', const=1, default=1,
                            help='If 0 is given, the browser window will be visible. It is hidden by default.')
    return parser


def default_date_format():
    return "%d/%m/%Y"