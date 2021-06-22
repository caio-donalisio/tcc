from pathlib import Path
from urllib.parse import urlparse


def store(path, contents):
  urlparts = urlparse(path)
  if urlparts.scheme == 'gs':
    store_on_gs(path, contents)
  elif urlparts.scheme == '':  # assume as local filesystem.
    store_on_fs(path, contents)


def store_on_gs(path, contents):
  # TODO: implement
  pass


def store_on_fs(path, contents):
  Path(path).parent.mkdir(parents=True, exist_ok=True)
  with open(path, 'w+') as f:
    f.write(contents)
    f.flush()