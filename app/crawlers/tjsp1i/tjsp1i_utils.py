import pathlib

from app.crawlers import utils
from google.cloud import storage
import random

client = storage.Client()

def list_all(bucket_name, prefix):
  bucket = utils.get_bucket_ref(bucket_name)
  for blob in bucket.list_blobs(prefix=prefix):
    yield blob.name

def list_pending_pdfs(bucket_name, prefix):
  metas = {}
  pdfs  = {}

  for name in list_all(bucket_name, prefix):
    path = pathlib.Path(name)
    if name.endswith(".html") and not name.endswith("SEC.html"):
      metas[path.stem] = path.parent
    if name.endswith(".pdf"):
      pdfs[path.stem] = path.parent

  bucket = client.get_bucket(bucket_name)
  meta_items = list(metas.items())
  random.shuffle(meta_items)
  for name, parent in meta_items:
    if name not in pdfs:
      yield  {
        'row':bucket.get_blob(f'{parent}/{name}.html').download_as_string(),
        'dest': f'{parent}/{name}'
      }

#Secmeta = Secondary metadata
def list_pending_secmetas(bucket_name, prefix):
  metas = {}
  secmetas  = {}

  for name in list_all(bucket_name, prefix):
    path = pathlib.Path(name)
    if name.endswith(".html") and not name.endswith("SEC.html"):
      metas[path.stem] = path.parent
    if name.endswith("SEC.html"):
      secmetas[path.stem] = path.parent

  bucket = client.get_bucket(bucket_name)
  for name, parent in metas.items():
    if name not in secmetas:
      _, cdProcesso, cdForo, __, ___, numProcesso  = name.split('-')
      yield {'url':f"https://esaj.tjsp.jus.br/cpopg/show.do?processo.codigo={cdProcesso}&processo.foro={cdForo}&processo.numero={numProcesso}",
         'dest': f'{parent}/{name}.pdf'
      }