from pathlib import Path
from urllib.parse import urlparse


import google.cloud.storage as gstorage

storage_client = None
buckets = {}


def get_storage_client():
  global storage_client
  if storage_client is None:
    storage_client = gstorage.Client()
  return storage_client


def get_bucket_ref(bucket_name):
  bucket = buckets.get(bucket_name)
  if not bucket:
    buckets[bucket_name] = get_storage_client().bucket(bucket_name)
  return buckets[bucket_name]


def store(path, contents):
  urlparts = urlparse(path)
  if urlparts.scheme == 'gs':
    store_on_gs(path, contents)
  elif urlparts.scheme == '':  # assume as local filesystem.
    store_on_fs(path, contents)


def store_on_gs(path, contents):
  urlparts = urlparse(path)
  bucket_name = urlparts.netloc
  blob_name = urlparts.path.lstrip('/')
  bucket = get_bucket_ref(bucket_name)
  blob = bucket.blob(blob_name)
  blob.upload_from_string(contents)


def store_on_fs(path, contents):
  Path(path).parent.mkdir(parents=True, exist_ok=True)
  with open(path, 'w+') as f:
    f.write(contents)
    f.flush()
