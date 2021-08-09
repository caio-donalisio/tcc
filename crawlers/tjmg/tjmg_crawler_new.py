import json
import time
import requests
import pendulum
import utils
import logging
import random
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm
import requests
import utils
import json
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
import time
import os
from urllib.parse import parse_qsl, urlencode, urlsplit
import speech_recognition as sr
from datetime import datetime, timedelta
from tqdm.contrib.logging import logging_redirect_tqdm
from urllib3.exceptions import InsecureRequestWarning
from utils import setup_logger
import click
from app import cli, celery
from logconfig import logger_factory
from crawlers.stf.stf_api_query import get_query


@celery.task(queue='crawlers', rate_limit='2/h', default_retry_delay=30 * 60,
             autoretry_for=(Exception,))
def tjmg_task(start_date, end_date, output_uri, pdf_async, skip_pdf):
  start_date, end_date =\
    pendulum.parse(start_date), pendulum.parse(end_date)

  output = utils.get_output_strategy_by_path(path=output_uri)
  logger = logger_factory('tjmg')
  logger.info(f'Output: {output}.')

  crawler = TJMG(params={
    'start_date': start_date, 'end_date': end_date
  }, output=output, logger=logger, pdf_async=pdf_async, skip_pdf=skip_pdf)
  crawler.run()

@cli.command(name='tjmg')
@click.option('--start-date', prompt=True,   help='Format YYYY-MM-DD.')
@click.option('--end-date'  , prompt=True,   help='Format YYYY-MM-DD.')
@click.option('--output-uri', default=None,  help='Output URI (e.g. gs://bucket_name')
@click.option('--pdf-async' , default=False, help='Download PDFs async'   , is_flag=True)
@click.option('--skip-pdf'  , default=False, help='Skip PDF download'     , is_flag=True)
@click.option('--enqueue'   , default=False, help='Enqueue for a worker'  , is_flag=True)
def stf_command(start_date, end_date, output_uri, pdf_async, skip_pdf, enqueue):
  args = (start_date, end_date, output_uri, pdf_async, skip_pdf)
  if enqueue:
    stf_task.delay(*args)
  else:
    tjmg_task(*args)