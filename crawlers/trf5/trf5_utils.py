import pathlib

import utils

def list_all(input_uri, prefix):
  import os

  if input_uri.startswith('gs://'):
    bucket = utils.get_bucket_ref(input_uri)
    for blob in bucket.list_blobs(prefix=prefix):
      yield blob.name
  else:
    for file in os.listdir(f'{input_uri}/{bool(prefix) * (prefix)}'):
      yield file


def list_pending_pdfs(input_uri, prefix):
  import json

  jsons = {}
  pdfs  = {}

  for name in list_all(input_uri, prefix):
    path = pathlib.Path(f'{input_uri}/{bool(prefix) * (prefix + "/")}{name}')
    if name.endswith(".json"):
      jsons[path.stem] = path.parent
    if name.endswith(".html") or name.endswith(".pdf"):
      pdfs[path.stem] = path.parent

  for name, parent in jsons.items():
    
    if name not in pdfs:
      
      with open(f'{parent}/{name}.json',encoding='latin-1') as f:
        row = json.loads(f.read())
      
      yield {
        'row':row,
        'dest': f'{bool(prefix) * (prefix + "/")}{name}'
      }