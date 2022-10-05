import os

from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from urllib.parse import urlencode

from selenium.webdriver.common.desired_capabilities import DesiredCapabilities


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


class FirefoxBrowser:
  def __init__(self, headers=None, headless=True,page_load_strategy='normal'):
    options = self._get_options(headers, headless,page_load_strategy)

    env = os.getenv('ENV', 'development')

    if env == "development":
        self.driver = webdriver.Firefox(options=options)
    else:
        self.driver = webdriver.Remote(
            command_executor='http://selenium-hub:4444/wd/hub',
            desired_capabilities=DesiredCapabilities.FIREFOX
        )

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

  def bsoup(self, parser='html.parser'):
    from bs4 import BeautifulSoup
    return BeautifulSoup(self.page_source(), parser)

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
    start_input.clear()
    start_input.send_keys(value)

  def select_by_id(self, field_id, option):
    self.driver.find_element_by_xpath(
      f"//select[@id='{field_id}']/option[text()='{option}']").click()

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

  def _get_options(self, headers, headless,page_load_strategy):
    options = Options()
    headers = urlencode(headers or self._sample_headers())
    options.add_argument(headers)
    if headless:
        options.add_argument('--headless')
    if page_load_strategy:
        options.add_argument('--page-load-strategy')
    return options

  def _sample_headers(self):
    return {'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) \
                        AppleWebKit/537.36 (KHTML, like Gecko) \
                        Chrome/91.0.4472.114 Safari/537.36'}