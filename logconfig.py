import logging
from google.cloud.logging import Client
from google.cloud.logging.handlers import CloudLoggingHandler


DEFAULT_FORMATTER =\
  '[%(asctime)s] %(levelname)s %(name)s: %(message)s'


class ContextFilter(logging.Filter):
  def filter(self, record):
    record.crawler_args = ''
    if hasattr(record, 'start_date') and hasattr(record, 'end_date'):
      record.crawler_args = f'start_date={record.start_date}, end_date={record.end_date}'
    return True


def setup_logger(logger, output, level=logging.INFO, cloud=True):
  formatter = logging.Formatter(DEFAULT_FORMATTER)

  file_handler = logging.FileHandler(f'logs/app.log')
  file_handler.setFormatter(formatter)
  logger.addHandler(file_handler)

  stream_handler = logging.StreamHandler()
  stream_handler.setFormatter(formatter)
  logger.addHandler(stream_handler)

  logger.setLevel(level)
  logger.propagate = False


def setup_cloud_logger(logger):
  formatter = logging.Formatter(DEFAULT_FORMATTER)

  client = Client()
  cloud_handler = CloudLoggingHandler(client)
  destination =\
    'logging.googleapis.com/projects/inspira-development/locations/us-east1/buckets/log-crawlers'
  client.sink('log-crawlers', destination=destination)
  cloud_handler.setFormatter(formatter)
  logger.addHandler(cloud_handler)


logging.getLogger('urllib3').setLevel(logging.WARNING)


def logger_factory(name, level=logging.INFO, output='workers.log'):
  logger = logging.getLogger(name)
  setup_cloud_logger(logger)
  setup_logger(logger, output=output, level=level)
  return logger