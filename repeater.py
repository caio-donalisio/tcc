import click
# Quick CLI to trigger crawlers indefinitely

@click.group()
def cli():
    pass

@cli.command(name='repeat')
@click.option('--court', prompt=True, help='Code of the court')
@click.option('--start-date', prompt=True,   help='Format YYYY-mm-dd')
@click.option('--end-date', prompt=True,   help='Format YYYY-mm-dd')
@click.option('--local', is_flag=True, default=False ,help='Saves collected data locally')
def repeat(court,start_date,end_date, local):
    import os
    from time import sleep
    if local:
        output_uri = f'./data/{court}'
    else:
        output_uri = f'gs://inspira-production-buckets-{court}'
    while True:
        os.system(f'python commands.py {court} --start-date {start_date} --end-date {end_date} --output-uri {output_uri}')
        sleep(15)

cli()

#python repeater.py repeat --court tjmg --start-date 2019-05-01 --end-date 2019-05-31
