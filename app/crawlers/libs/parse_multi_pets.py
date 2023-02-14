import sys
import os
from datetime import datetime
import time
import json
from bs4 import BeautifulSoup
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import logging


def download_pdf_file(link, filename):
  r = ''

  for retries in range(5):
    try:
      print("Trying to GET %s" % (link))
      r = requests.get(link, timeout=(2*(retries+1)))
      # print("Out of GET")
    except:
      # requests.exceptions.Timeout:
      # There are all kinds of exceptions here like OpenSSL.SSL.WantReadError that can trigger before Timeout exception
      # Let's just catch everything and assume that it's some kind of connection / ssl / read error and treat them all the same
      sleep_time = 5 * (retries+1)
      logging.error("[-] Got a timeout while requesting pdf. Backing off for %d seconds and trying again" % (sleep_time))
      time.sleep(sleep_time)
      continue

    if r.status_code == 200 and not 'Erro ao gerar arquivo do inteiro teor' in r.text:  # Will this work?
      break
    else:
      if 'Erro ao gerar arquivo do inteiro teor' in r.text:
        logging.error("[-] Ok we found an Erro ao gerar arquivo do inteiro teor error! \o/ Go Team \o/")
      else:
        logging.error(
            "[-] Got a weird status code %d while requesting first_stage_url. Backing off for five seconds and trying again" % (r.status_code))
        time.sleep(5)
        continue

  if r == '' or r.status_code != 200 or 'Erro ao gerar arquivo do inteiro teor' in r.text:
    logging.error("Unable to access first_stage_url after 5 tries. Letting watchdog take care of us.")
    driver.quit()
    sys.exit()

  if r.headers['Content-Type'] == 'application/pdf':
    f = open(filename, "wb")
    f.write(r.content)
    f.flush()
    f.close()
    print("[+] SUCCESS")
  else:
    logging.error("[-] Unknown content type header in HTTP reponse")


def handle_multiple_petitions(content, text):

  filename_array = []

  soup = BeautifulSoup(text, 'html.parser')

  pets_div = soup.find('div', {'id': 'listaInteiroTeor'})

  pets_rows = pets_div.find_all('div', {'class': 'row pt-2 pb-2'})

  for pet_row in pets_rows:
    # print(pet_row)
    link_div = pet_row.find('div', {'class': 'col-md-3'})
    link_a = pet_row.find_all('a')
    # print(link_a[0])
    dl_link = link_a[0]['href']

    # print(dl_link)

    get_params = dl_link.split('?')[1]
    get_attrs = get_params.split('&')
    # print(get_attrs)
    nreg = get_attrs[2].split('=')[1]
    seq = get_attrs[0].split('=')[1]

    dl_file_name = "downloaded_pdfs/%s-%s.pdf" % (seq, nreg)

    # print(dl_file_name)

    download_pdf_file(dl_link, dl_file_name)

    filename_array.append(dl_file_name)

  return ','.join(filename_array)


if __name__ == "__main__":
  f = open('stj_multi_pets.html', 'rb')
  html_data = f.read()
  handle_multiple_petitions(html_data, html_data)
