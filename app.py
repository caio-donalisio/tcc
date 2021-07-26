import conf
import click
from celery import Celery
from celery.signals import setup_logging

from logconfig import logger_factory

if conf.get('SENTRY_DSN'):
  import sentry_sdk
  sentry_sdk.init(
    conf.get('SENTRY_DSN'),
    traces_sample_rate=1.0
  )

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