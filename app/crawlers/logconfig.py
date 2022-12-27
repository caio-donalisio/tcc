import os
import logging
from google.cloud.logging import Client
from google.cloud.logging.handlers import CloudLoggingHandler

logging.getLogger('urllib3').setLevel(logging.WARNING)


DEFAULT_FORMATTER =\
  '[%(asctime)s] p%(process)s %(module)s %(levelname)s %(name)s: %(message)s'


from app.crawlers.logutils import logging_context_handler

class ContextFilter(logging.Filter):
  def filter(self, record):
    record.crawler =\
      (logging_context_handler.get('crawler') or 'unknown')
    return True


def setup_logger(logger, level=logging.INFO, cloud=True):
  formatter = logging.Formatter(DEFAULT_FORMATTER)

  if os.environ.get('LOG_FILE', 'true') == 'true':
    file_handler = logging.FileHandler('logs/app.log')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

  stream_handler = logging.StreamHandler()
  stream_handler.setFormatter(formatter)
  logger.addHandler(stream_handler)

  logger.setLevel(level)
  logger.propagate = False


def setup_cloud_logger(logger):
  if not os.getenv('CLOUD_LOGGING_ENABLED'):
    return

  print("Notice: CLOUD_LOGGING_ENABLED enabled.")

  formatter = logging.Formatter(DEFAULT_FORMATTER)

  client = Client()
  cloud_handler = CloudLoggingHandler(client)
  destination =\
    'logging.googleapis.com/projects/inspira-development/locations/us-east1/buckets/log-crawlers'
  client.sink('log-crawlers', destination=destination)
  cloud_handler.setFormatter(formatter)
  logger.addHandler(cloud_handler)


def logger_factory(name, level=logging.INFO):
  logger = logging.getLogger(name)
  context_filter = ContextFilter()
  logger.addFilter(context_filter)

  setup_cloud_logger(logger)
  setup_logger(logger, level=level)
  return logger


logger = logger_factory('crawlers')
