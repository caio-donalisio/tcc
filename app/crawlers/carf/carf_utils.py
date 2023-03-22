import pathlib

from app.crawlers import utils
from google.cloud import storage

client = storage.Client()

def list_all(bucket_name, prefix):
  bucket = utils.get_bucket_ref(bucket_name)
  for blob in bucket.list_blobs(prefix=prefix):
    yield blob.name

def file_is_invalid(blob):
    return blob.name.endswith('.pdf') and not 'PDF-1.' in str(blob.download_as_bytes())

def list_invalid_files(bucket_name, prefix):
  bucket = utils.get_bucket_ref(bucket_name)
  
  for n, blob in enumerate(bucket.list_blobs(prefix=prefix), start=1):
    print(n)
    if file_is_invalid(blob):
      print('INVÁLIDO')
      path = pathlib.Path(blob.name)
      name , ext = path.name.split('.')
      assert len(name.split('_')) == 4
      parent = path.parent 
      yield {'row':bucket.get_blob(f'{parent}/{name}.json').download_as_string(),
          'dest': f'{parent}/{name}'
      }
      
  