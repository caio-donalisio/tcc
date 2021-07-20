from app import celery

import requests
import utils

# disable some warnings related to verify as False.
from urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

from crawlers.trf2.trf2_crawler import trf2_task
from crawlers.stf.stf_api_crawler import stf_task
from crawlers.tjsp2.tjsp_crawler import tjsp_task
from crawlers.tjrj.tjrj_crawler import tjrj_task


@celery.task(queue='downloader', trail=True, rate_limit='120/m')
def download_from_url(url, dest, output_uri, **kwargs):
  output   = utils.get_output_strategy_by_path(path=output_uri)

  if kwargs.get('override', True) is False and \
    output.exists(dest):
    return

  response = requests.get(url,
    allow_redirects=True,
    verify=kwargs.get('verify_ssl', False),
    headers=kwargs.get('headers', {}))

  if response.status_code == 404 and \
    kwargs.get('ignore_not_found', False):
    return

  assert response.status_code == 200

  output.save_from_contents(
    filepath=dest,
    contents=response.content,
    mode=kwargs.get('write_mode', 'w'),
    content_type=kwargs.get('content_type', 'text/plain'))