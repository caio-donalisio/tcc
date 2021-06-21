import math
import pendulum
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport


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
    params = {
      'decisaoFilter': filters,
      'pageNumber': page_number,
      'itemsPerPage': items_per_page,
    }
    return self.gql_client.execute(graphql_query, variable_values=params)

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