#!/bin/sh
celery -A tasks flower --port=5555 --detach