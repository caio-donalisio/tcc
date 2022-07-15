import pathlib

import utils

def list_all(bucket_name, prefix):

  import os
  folder = bucket_name
  for file in os.listdir(folder + bool(prefix) * f'/{prefix}'):
    yield file


def list_pending_pdfs(bucket_name, prefix):
  htmls = {}
  pdfs  = {}
  b_files = {}

  for name in list_all(bucket_name, prefix):
    path = pathlib.Path(f'{bucket_name}/{name}')
    if name.endswith("A.html"):
      htmls[path.stem] = path.parent
    if name.endswith("B.html"):
      b_files[path.stem] = path.parent
    if name.endswith(".pdf"):
      pdfs[path.stem] = path.parent

  for name, parent in htmls.items():
    if name not in pdfs: # name not in b_files:
      with open(f'{parent}/{bool(prefix) * (prefix + "/")}{name}.html',encoding='latin-1') as f:
        row = f.read()
      yield {'row':row,
         'dest': f'{parent}/{name[:name.find("_A")]}'
      }