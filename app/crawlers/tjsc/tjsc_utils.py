import pathlib

from app.crawlers import utils
from google.cloud import storage

client = storage.Client()

def list_all(bucket_name, prefix):
  bucket = utils.get_bucket_ref(bucket_name)
  for blob in bucket.list_blobs(prefix=prefix):
    yield blob.name

def file_is_invalid(blob):
    return blob.name.endswith('FULL.html') and blob.size < 7900

def list_invalid_files(bucket_name, prefix):
  bucket = utils.get_bucket_ref(bucket_name)
  
  for blob in bucket.list_blobs(prefix=prefix):
    if file_is_invalid(blob):
      path = pathlib.Path(blob.name)
      *els , _ = path.name.split('_')
      assert len(els) == 3
      name = '_'.join(els)
      parent = path.parent 
      yield {'row':bucket.get_blob(f'{parent}/{name}.html').download_as_string(),
          'dest': f'{parent}/{name}'
      }
      
  