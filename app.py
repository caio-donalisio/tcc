import click
import sentry_sdk
from celery import Celery
from celery.schedules import crontab
from celery.signals import setup_logging, worker_process_init, worker_ready
from celery_singleton import clear_locks
from sentry_sdk.integrations.celery import CeleryIntegration

import conf
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


@celery.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    from crawlers.tjsp.tjsp_crawler import tjsp_task_download_from_prev_days
    from crawlers.tjsp.tjsp_pdf import tjsp_download_task

    sender.add_periodic_task(
      crontab(minute=0, hour=3),
      tjsp_task_download_from_prev_days.s(
        output_uri='gs://inspira-tjsp',
        max_prev_days=1,
      ),
    )
