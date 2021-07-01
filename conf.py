import os
from dotenv import load_dotenv
load_dotenv()
if os.environ.get('ENV'):
  load_dotenv('.env.{}'.format(os.environ.get('ENV')), override=True)


def get(name, default=None):
  return os.environ.get(name, default=default)


from logging.config import dictConfig

dictConfig({
    'version': 1,
    'formatters': {'default': {
        'format': '[%(asctime)s] %(levelname)s in %(module)s: %(message)s',
    }},
    'handlers': {
      'console': {
        'class': 'logging.StreamHandler',
        'formatter': 'default'
      },
      'file': {
        'class': 'logging.FileHandler',
        'filename': 'logs/crawlers.log',
        'formatter': 'default'
      },
    },
    'root': {
        'level': 'DEBUG',
        'handlers': ['console', 'file']
    }
})