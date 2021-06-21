import click
import pendulum

import tasks


@click.group()
@click.pass_context
def cli(ctx):
  pass


@cli.command()
@click.option('--start-date', prompt=True, help='Start date (format YYYY-MM-DD).')
@click.option('--end-date'  , prompt=True, help='End date (format YYYY-MM-DD).')
@click.option('--items-per-page', default=50, help='Page size')
@click.option('--chunk-size', default=5, help='Number of pages per subtask')
def tjba(start_date, end_date, items_per_page, chunk_size):
  from tqdm import tqdm
  from crawlers.tjba.tjba_crawler import get_filters, process_tjba

  filters = get_filters(
      start_date=pendulum.parse(start_date), end_date=pendulum.parse(end_date))
  results = process_tjba.delay(
    filters=filters, items_per_page=items_per_page, chunk_size=chunk_size)

  total = 0
  for result in tqdm(results.collect()):
    result_type, values = result
    if result_type.__class__.__name__ == 'GroupResult':
      total += sum(values)
  print('done', total)


@cli.command()
def tjsp():
  pass


if __name__ == '__main__':
  cli(obj={})  # type: ignore