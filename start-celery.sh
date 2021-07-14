#!/bin/sh
CELERYD_HIJACK_ROOT_LOGGER=false celery --config=celeryconf -A tasks worker -Q persistence,crawlers,downloader --concurrency=2 -E --loglevel=INFO --detach;
