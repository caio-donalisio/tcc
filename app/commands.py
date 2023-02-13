from app.crawler_cli import cli


@cli.command(name='clear-locks')
def clear_celery_singleton_locks():
  print('OK')
  from celery_singleton import clear_locks

  from app.celery_run import celery_app
  clear_locks(celery_app)
  print('OK')


if __name__ == '__main__':
  cli(obj={})  # type: ignore
