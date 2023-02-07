#!/bin/sh
celery -A app.celery_run.celery_app flower --port=5555 --detach
