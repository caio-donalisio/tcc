from os import name

import click
import pendulum

from app import cli
from crawlers.scrfb.scrfb_crawler import scrfb_command
from crawlers.carf.carf_crawler import carf_command
from crawlers.stf.stf_api_crawler import stf_command
from crawlers.stj.stj_crawler import stj_command
from crawlers.titsp.titsp_crawler import titsp_command
from crawlers.tjba.tjba_crawler import tjba_command
from crawlers.tjmg.tjmg_crawler import tjmg_command
from crawlers.tjrj.tjrj_crawler import tjrj_command
from crawlers.tjrs.tjrs_crawler import tjrs_command
from crawlers.tjsp.tjsp_crawler import tjsp_command, tjsp_validate
from crawlers.tjsp.tjsp_pdf import tjsp_pdf_command
from crawlers.trf1.trf1_pdf import trf1_pdf_command
from crawlers.trf1.trf1_crawler import trf1_command
from crawlers.trf2.trf2_crawler import trf2_command
from crawlers.trf3.trf3_crawler import trf3_command
from crawlers.trf4.trf4_crawler import trf4_command
from crawlers.trf5.trf5_crawler import trf5_command
from crawlers.tst.tst_crawler import tst_command
from crawlers.tjpr.tjpr_crawler import tjpr_command
from crawlers.tjdf.tjdf_crawler import tjdf_command

@cli.command(name='clear-locks')
def clear_celery_singleton_locks():
  from celery_singleton import clear_locks

  from app import celery
  clear_locks(celery)
  print('OK')


if __name__ == '__main__':
  cli(obj={})  # type: ignore
