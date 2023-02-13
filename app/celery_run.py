import os

from celery import Celery


def make_celery() -> Celery:
  celery = Celery()
  environment = os.getenv("ENV", "development")
  celery.config_from_object(f"app.config.celery.{environment}", force=True)

  print(f"environment = {environment}")

  return celery


celery_app = make_celery()

__all__ = ("celery_app", )
