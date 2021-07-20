import conf
import click
from celery import Celery
from celery.signals import setup_logging

from logconfig import logger_factory


celery = Celery('inspira',
  broker=conf.get('CELERY_BROKER_URL'),
  backend=conf.get('CELERY_BACKEND_URL'))
celery.config_from_object('celeryconf')


@setup_logging.connect
def on_setup_logging(**kwargs):
  logger_factory('celery')


@click.group()
@click.pass_context
def cli(ctx):
  pass