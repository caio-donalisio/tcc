import conf
import click

from celery import Celery
from celery.signals import setup_logging, worker_process_init, worker_ready
from celery_singleton import clear_locks


import sentry_sdk
from sentry_sdk.integrations.celery import CeleryIntegration

from logconfig import logger_factory

sentry_config = dict(
  traces_sample_rate=1.0,
  integrations=[CeleryIntegration()])

if conf.get('SENTRY_DSN'):
  sentry_sdk.init(conf.get('SENTRY_DSN'), **sentry_config)

celery = Celery('inspira',
  broker=conf.get('CELERY_BROKER_URL'),
  backend=conf.get('CELERY_BACKEND_URL'))
celery.config_from_object('celeryconf')


@setup_logging.connect
def on_setup_logging(**kwargs):
  logger_factory('celery')


@worker_process_init.connect
def on_worker_process_init(**kwargs):
  if conf.get('SENTRY_DSN'):
    sentry_sdk.init(conf.get('SENTRY_DSN'), **sentry_config)


@worker_ready.connect
def unlock_all(**kwargs):
  clear_locks(celery)


@click.group()
@click.pass_context
def cli(ctx):
  pass