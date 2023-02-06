#!/bin/sh

CONCURRENCY=${1:-2}

CELERYD_HIJACK_ROOT_LOGGER=false celery -A app.celery_run.celery_app worker --concurrency="${CONCURRENCY}" -X crawler-queue -l debug -E
