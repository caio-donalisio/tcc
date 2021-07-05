from os import name
import click
import pendulum
from app import cli

from crawlers.trf2.trf2_crawler import (
  trf2_command)
from crawlers.stf.stf_api_crawler import (
  stf_command)
from crawlers.tjsp2.tjsp_crawler import (
  tjsp_command)


@cli.command()
@click.option('--start-date', prompt=True, help='Start date (format YYYY-MM-DD).')
@click.option('--end-date'  , prompt=True, help='End date (format YYYY-MM-DD).')
@click.option('--items-per-page', default=50, help='Page size')
@click.option('--chunk-size', default=5, help='Number of pages per subtask')
@click.option('--output-uri', default=None, help='Output URI')
def tjba(start_date, end_date, items_per_page, chunk_size, output_uri):
  from tqdm import tqdm
  from crawlers.tjba.tjba_crawler import process_tjba

  results = process_tjba.delay(
    start_date=start_date,
    end_date=end_date,
    items_per_page=items_per_page,
    chunk_size=chunk_size,
    output_uri=(output_uri.rstrip('/') if output_uri else 'data/tjba'))

  total = 0
  for result in tqdm(results.collect()):
    result_type, values = result
    if result_type.__class__.__name__ == 'GroupResult' and \
        all([isinstance(val, int) for val in values]):
      total += sum(values)
  print('done', total)


if __name__ == '__main__':
  cli(obj={})  # type: ignore