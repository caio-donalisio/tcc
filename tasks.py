from app import celery
import crawlers.tjba.tjba_crawler

import requests
import utils

# disable some warnings related to verify as False.
from urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

import logging
logger = logging.getLogger(__name__)


@celery.task(queue='downloader', trail=True, rate_limit='120/m')
def download_from_url(url, dest, output_uri, **kwargs):
  output   = utils.get_output_strategy_by_path(path=output_uri)
  response = requests.get(url,
    allow_redirects=True,
    verify=kwargs.get('verify_ssl', False),
    headers=kwargs.get('headers', {}))
  assert response.status_code == 200
  output.save_from_contents(
    filepath=dest,
    contents=response.content,
    mode=kwargs.get('write_mode', 'w'),
    content_type=kwargs.get('content_type', 'text/plain'))