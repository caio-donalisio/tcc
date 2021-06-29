import conf
import click
from celery import Celery


celery = Celery('inspira',
  broker=conf.get('CELERY_BROKER_URL'),
  backend=conf.get('CELERY_BACKEND_URL'))


@click.group()
@click.pass_context
def cli(ctx):
  pass
