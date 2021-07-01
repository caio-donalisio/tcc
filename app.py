import conf
import click
from celery import Celery
from celery.signals import after_setup_logger, after_setup_task_logger

import logging

from logconfig import setup_logger


celery = Celery('inspira',
  broker=conf.get('CELERY_BROKER_URL'),
  backend=conf.get('CELERY_BACKEND_URL'))


@after_setup_logger.connect
def setup_loggers(logger, *args, **kwargs):
  logger.handlers.clear()
  setup_logger(logger, output='workers.log')


@after_setup_task_logger.connect
def setup_task_logger(**kw):
  logger = logging.getLogger('tasks')
  logger.handlers.clear()
  setup_logger(logger, output='workers.log')


@click.group()
@click.pass_context
def cli(ctx):
  pass