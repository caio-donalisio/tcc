import conf
import click
from celery import Celery
from celery.signals import after_setup_logger, after_setup_task_logger

from logconfig import setup_logger, setup_cloud_logger


celery = Celery('inspira',
  broker=conf.get('CELERY_BROKER_URL'),
  backend=conf.get('CELERY_BACKEND_URL'))


@after_setup_logger.connect
def setup_loggers(logger, *args, **kwargs):
  setup_logger(logger, output='workers.log')
  setup_cloud_logger(logger)


@after_setup_task_logger.connect
def setup_task_logger(logger, *args, **kwargs):
  setup_logger(logger, output='workers.log')
  setup_cloud_logger(logger)


@click.group()
@click.pass_context
def cli(ctx):
  pass