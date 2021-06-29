import json
import math
import pendulum
import storage

from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport

from app import celery
from celery import group, chain

import logging
logger = logging.getLogger(__name__)


with open(f'crawlers/tjba/query.graphql', 'r') as f:
  graphql_query = gql(f.read())


def get_filters(start_date : pendulum.DateTime, end_date : pendulum.DateTime):
  return {
    'assunto': 'a OR o OR de OR por',
    'orgaos': [],
    'relatores': [],
    'classes': [],
    'dataInicial': start_date.start_of('day').to_iso8601_string(),
    'dataFinal': end_date.end_of('day').to_iso8601_string(),
    'segundoGrau': True,
    'turmasRecursais': True,
    'ordenadoPor': 'dataPublicacao'
  }


class TJBAClient:
  def __init__(self):
    self.transport  = RequestsHTTPTransport(url="https://jurisprudenciaws.tjba.jus.br/graphql")
    self.gql_client = Client(transport=self.transport)

  def count(self, filters):
    result = self.fetch(filters, page_number=0, items_per_page=1)
    return result['filter']['itemCount']

  def fetch(self, filters, page_number=0, items_per_page=10):
    try:
      params = {
        'decisaoFilter': filters,
        'pageNumber': page_number,
        'itemsPerPage': items_per_page,
      }
      return self.gql_client.execute(graphql_query, variable_values=params)
    except Exception as e:
      logger.error(f"page fetch error params: {params}")
      raise e

  def paginator(self, filters, items_per_page=10):
    item_count = self.count(filters)
    page_count = math.ceil(item_count / items_per_page)
    return Paginator(self, filters=filters, item_count=item_count, page_count=page_count,
      items_per_page=items_per_page)


class Paginator:
  def __init__(self, client, filters, item_count, page_count, items_per_page=10):
    self.client = client
    self._filters = filters
    self._item_count = item_count
    self._page_count = page_count
    self._items_per_page = items_per_page

  @property
  def total(self):
    return self._item_count

  @property
  def pages(self):
    return self._page_count

  def page(self, number):
    return self.client.fetch(
      filters=self._filters, page_number=number, items_per_page=self._items_per_page)

  def __repr__(self):
    return f'Paginator(item_count={self._item_count}, page_count={self._page_count})'


@celery.task(queue='crawlers', trail=True)
def process_tjba(start_date, end_date, items_per_page=50, chunk_size=5, output_uri=None):
  period   = pendulum.parse(start_date).diff(pendulum.parse(end_date))
  subtasks = [
    process_tjba_period.s(get_filters(start_date=day, end_date=day), items_per_page, chunk_size, output_uri)
    for day in period.range(unit='days')
  ]
  return group(subtasks).apply_async()


@celery.task(queue='crawlers', trail=True, rate_limit='120/m')
def process_tjba_period(filters, items_per_page=50, chunk_size=5, output_uri=None):
  client     = TJBAClient()
  paginator  = client.paginator(filters, items_per_page=items_per_page)

  total = 0
  for page_number in range(paginator.pages):
    result = client.fetch(
      filters=filters, page_number=page_number, items_per_page=items_per_page)
    docs = result['filter']['decisoes']
    total += len(docs)

    persist_tjba_page.delay(docs, output_uri)

  return total


@celery.task(queue='persistence', trail=True)
def persist_tjba_page(records, output_uri):
  for record in records:
    published_at = pendulum.parse(record['dataPublicacao'])
    doc_hash = record['hash']
    doc_id   = record['id']

    path = f"{output_uri}/{published_at.year}/{'{:02d}'.format(published_at.month)}/doc_{doc_id}_{doc_hash}.json"
    storage.store(path=path, contents=json.dumps(record))