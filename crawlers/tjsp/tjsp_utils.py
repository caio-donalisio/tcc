import pathlib

import utils

def list_all(bucket_name, prefix):
  bucket = utils.get_bucket_ref(bucket_name)
  for blob in bucket.list_blobs(prefix=prefix):
    yield blob.name


def list_pending_pdfs(bucket_name, prefix):
  jsons = {}
  pdfs  = {}

  for name in list_all(bucket_name, prefix):
    path = pathlib.Path(name)
    if name.endswith(".json"):
      jsons[path.stem] = path.parent
    if name.endswith(".pdf"):
      pdfs[path.stem] = path.parent

  for name, parent in jsons.items():
    if name not in pdfs:
      cdacordao = name.split('_')[-1]
      yield {'url':
        f'http://esaj.tjsp.jus.br/cjsg/getArquivo.do?conversationId=&cdAcordao={cdacordao}&cdForo=0',
         'dest': f'{parent}/{name}.pdf'
      }