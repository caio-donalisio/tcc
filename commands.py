from os import name
import click
import pendulum
from app import cli

from crawlers.trf2.trf2_crawler import (
  trf2_command)
from crawlers.trf4.trf4_crawler import (
  trf4_command)
from crawlers.stf.stf_api_crawler import (
  stf_command)
from crawlers.tjsp2.tjsp_crawler import (
  tjsp_command)
from crawlers.tjrj.tjrj_crawler import (
  tjrj_command)
from crawlers.tjba.tjba_crawler import (
  tjba_command)
from crawlers.tjsp2.tjsp_pdf import tjsp_pdf_command


if __name__ == '__main__':
  cli(obj={})  # type: ignore