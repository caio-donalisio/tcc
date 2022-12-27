import pathlib
from app.crawlers import utils
from google.cloud import storage

def list_all(bucket_name, prefix):
  bucket = utils.get_bucket_ref(bucket_name)
  for blob in bucket.list_blobs(prefix=prefix):
    yield blob.name

def list_pending_pdfs(bucket_name, prefix):
  import json

  jsons = {}
  inteiros  = {}

  for name in list_all(bucket_name, prefix):
      path = pathlib.Path(name)
      if name.endswith(".json"):
        jsons[path.stem] = path.parent
      if name.endswith(".pdf") or name.endswith(".html"):
        inteiros[path.stem] = path.parent

  client = storage.Client()
  bucket = client.get_bucket(bucket_name)

  counter = 0 
  for name, parent in jsons.items():
    
    
    if name not in inteiros and counter <= 250:
      counter += 1
      
      yield {
        'row':json.loads(bucket.get_blob(f'{parent}/{name}.json').download_as_string()),
       'dest': f'{parent}/{name}'
      }