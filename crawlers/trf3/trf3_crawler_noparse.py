#!/usr/bin/env python3
# -*- coding: latin-1 -*-

import sys
import requests
from bs4 import BeautifulSoup
import mimetypes
import re
import sqlite3
import json
import timeit
import pendulum
import traceback
from google.cloud import storage
import os

storage_client = storage.Client()

bucket_name = os.environ.get('BUCKET_NAME') or "trf3"

bucket = storage_client.create_bucket(bucket_name)

def uploadGCS(file_path, blob_name):
  global bucket
  blob = bucket.blob(blob_name)
  blob.upload_from_filename(file_path)
  print("uploaded (?)")

sys.path.append('../libs/')

from json_to_sqlite import json_to_sqlite_single

s = requests.Session()
s.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:84.0) Gecko/20100101 Firefox/84.0'})

# s.proxies = {'http': 'http://127.0.0.1:8080'}
errors = 0
good = 0
current_range = []
start = timeit.default_timer()

def initialize_session():
  r = s.get('http://web.trf3.jus.br/base-textual')
  return r.status_code == 200

def set_search(term, date_range):
  data = {
    'txtPesquisaLivre': term,
    'chkMostrarLista': 'on',
    'numero': '',
    'magistrado': 0,
    'data_inicial': date_range[0],
    'data_final': date_range[1],
    'data_tipo': 0,
    'classe': 0,
    'numclasse': '',
    'orgao': 0,
    'ementa': '',
    'indexacao': '',
    'legislacao': '',
    'chkAcordaos': 'on',
    'hdnMagistrado': '',
    'hdnClasse': '',
    'hdnOrgao': '',
    'hdnLegislacao': '',
    'hdnMostrarListaResumida': ''
  }
  r = s.post('http://web.trf3.jus.br/base-textual/Home/ResultadoTotais', data=data)
  return r.status_code == 200

def get_page(page):
  if page == 0:
    page = '1?np=0'
  else:
    page = f'3?np={page-1}'
  r = s.get(f'http://web.trf3.jus.br/base-textual/Home/ListaResumida/{page}')
  soup = BeautifulSoup(r.text, features="html.parser")
  rows = soup.find_all('tr')
  print(list(map(lambda el: el.find('a')['href'], rows)))

def get_acordao(id):
  global current_range
  try:
    r = s.get(f'http://web.trf3.jus.br/base-textual/Home/ListaColecao/9?np={id}')
    if 'Object reference not set to an instance of an object.' in r.text:
      return 'finish'
    if r.status_code != 200:
      with open('errors.txt', 'a+') as f:
        f.write(f'StatusCode: {r.status_code} / {id}')
        return False
    # print(r.text)
    # print(r.text)
    if 'Object reference not set to an instance of an object.' in r.text or 'expirou' in r.text:
      print('session expired, resetting search')
      initialize_session()
      set_search('a ou de ou o', current_range)
      r = s.get(f'http://web.trf3.jus.br/base-textual/Home/ListaColecao/9?np={id}')
      # print(r.text)
    # soup = BeautifulSoup(r.text, features="html.parser")
    # processo_full = soup.find('p', {'class': 'docTexto'}).get_text().strip()
    # tipo_processual = ' - '.join(processo_full.split(' - ')[:-1]).strip()
    # numero_acordao = processo_full.split(' - ')[-1].split('   ')[0].strip()
    numero_processo = processo_full.split(' - ')[-1].split('   ')[1].strip()
    numero_processo = re.sub(r'_|\.|-', '', numero_processo) # Sanitize (take dots, underlines and dashes out)
    # info = soup.find_all('div', {'class': 'docTexto'})
    # relator = info[0].get_text().strip()
    # orgao = info[1].get_text().strip()
    # data_julgamento = info[2].get_text().strip()
    # data_publicacao = re.search(r'\d*\/\d*\/\d*', info[3].get_text().strip()).group(0)
    # ementa = info[4].get_text().strip()
    # acordao = info[5].get_text().strip()
    # integra = soup.find('div', {'id': 'acoesdocumento'}).find('a')['x']
    doc_urls = get_docs(integra, id)
  
    downloaded = download_docs(doc_urls, numero_processo) if doc_urls else []
    return { 'num': numero_processo, 'data': r.text }
    # downloaded = False

    # print(','.join(downloaded))

    # df = {'Tipo Processual': tipo_processual, 'NumProcesso': numero_processo, 'Relator(a)': relator, 'Orgao Julgador': orgao, 'Data da Publicacao': data_publicacao, 'Data do Julgamento': data_julgamento, 'Ementa': ementa, 'Acordao': acordao, 'PathToPdf': ','.join(downloaded)}

    # return df
  except:
    global errors
    print('error')
    with open('errors.txt', 'a+') as f:
      r = s.get(f'http://web.trf3.jus.br/base-textual/Home/ListaColecao/9?np={id}')
      f.write(str(id) + f'\n=========\n{r.text}\n\n\n\n\n{traceback.format_exc()}\n============\n')
    errors += 1

def get_docs(url, id):
  r = s.get(url)
  # print(r.text)
  if 'Ocorreu um erro' in r.text:
    with open('errors.txt', 'a+') as f:
      f.write(f'DocError: {url} / {id}')
    return False
  soup = BeautifulSoup(r.text, features="html.parser")
  links = list(map(lambda el: 'http://web.trf3.jus.br' + el['href'], soup.find('form').find('ul').find_all('a')))
  return links

def download_docs(links, numero_processo):
  files = []
  for idx, link in enumerate(links):
    r = s.get(link)
    extension = mimetypes.guess_extension(r.headers['content-type'].split(';')[0]) # get extension from mime
    suffix = f'_{idx+1}'
    filename = f'{numero_processo}{suffix}{extension}'
    filepath = f'downloads/{filename}'
    with open(filepath, 'wb') as file:
      file.write(r.content)
      files.append(filepath)
      print(f'{filename} downloaded.')
      uploadGCS(filepath, filename)
    return files

start_date = pendulum.datetime(2020, 1, 1)
end_date = pendulum.datetime(2021, 1, 1)

period = pendulum.period(start_date, end_date)

dates = period.range("months")

ranges = []

for dt in dates:
  ranges.append([dt.format('DD/MM/YYYY'), dt.add(months=1).format('DD/MM/YYYY')])

print(initialize_session())

checkpoint = '0:0'

with open('checkpoint.txt', 'r') as f:
  checkpoint = f.readline()

dateid = int(checkpoint.split(':')[0])
offset = int(checkpoint.split(':')[1])

ranges = ranges[dateid:]

for idx, date_range in enumerate(ranges):
  current_range = date_range
  print(f'using range: {"-".join(date_range)}')
  print(set_search('a ou de ou o', date_range))
  i = offset if offset else 1 # offset from the actual search
  while True:
    acordao = get_acordao(i)

    if acordao == 'finish':
      break

    if acordao:
      good += 1
      filename = f'{acordao.num}.html'
      filepath = f'raw_html/{acordao.num}.html' 
      with open(filepath) as file:
        file.write(acordao.data)
      uploadGCS(filepath, filename)

    stop = timeit.default_timer()
    idstr = f'{idx}:{i}'
    print(f'Current ID: {idstr} | Total errors: {errors} | Total processed: {good} | Time elapsed: {stop - start}')
    with open('checkpoint.txt', 'w') as f:
      f.write(idstr)
    i += 1