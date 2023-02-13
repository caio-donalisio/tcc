import os
from dotenv import load_dotenv
load_dotenv()
if os.environ.get('ENV'):
  load_dotenv('.env.{}'.format(os.environ.get('ENV')), override=True)


def get(name, default=None):
  return os.environ.get(name, default=default)
