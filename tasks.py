import conf
from celery import Celery, group
from crawlers.tjba.tjba_crawler import TJBAClient

import logging
logger = logging.getLogger(__name__)


celery = Celery('inspira',
  broker=conf.get('CELERY_BROKER_URL'),
  backend=conf.get('CELERY_BACKEND_URL'))


@celery.task(queue='crawlers', trail=True)
def process_tjba(filters, items_per_page=50, chunk_size=5):
  paginator  = TJBAClient().paginator(filters, items_per_page=items_per_page)
  page_count = paginator.pages
  item_count = paginator.total
  pages = list(range(page_count))

  logger.info("@process_tjba - item_count", item_count, "page_count", page_count)

  chunks = [
    pages[i:i + chunk_size] for i in range(0, len(pages), chunk_size)]

  return group([
    process_tjba_page.s(chunk, filters, items_per_page)
    for chunk in chunks
  ]).apply_async()


@celery.task(queue='crawlers', trail=True, rate_limit='1/s',
  autoretry_for=(Exception,), retry_backoff=True, retry_jitter=30)
def process_tjba_page(chunk, filters, items_per_page):
  logger.info("@process_tjba - chunk: ", chunk)

  total   = 0
  client  = TJBAClient()
  # TODO: estou fazendo nada ainda
  for page_number in chunk:
    result = client.fetch(
      filters=filters, page_number=page_number, items_per_page=items_per_page)
    total += len(result['filter']['decisoes'])
  return total