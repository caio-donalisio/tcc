import sentry_sdk
from celery.signals import setup_logging, worker_process_init
from sentry_sdk.integrations.celery import CeleryIntegration

from app import conf
from app.crawlers.logconfig import logger_factory

sentry_config = dict(
    traces_sample_rate=1.0,
    integrations=[CeleryIntegration()])

if conf.get('SENTRY_DSN'):
  sentry_sdk.init(conf.get('SENTRY_DSN'), **sentry_config)


@setup_logging.connect
def on_setup_logging(**kwargs):
  logger_factory('celery')


@worker_process_init.connect
def on_worker_process_init(**kwargs):
  if conf.get('SENTRY_DSN'):
    sentry_sdk.init(conf.get('SENTRY_DSN'), **sentry_config)
