import logging
from google.cloud.logging import Client
from google.cloud.logging.handlers import CloudLoggingHandler


class ContextFilter(logging.Filter):
  def filter(self, record):
    record.crawler_args = ''
    if hasattr(record, 'start_date') and hasattr(record, 'end_date'):
      record.crawler_args = f'start_date={record.start_date}, end_date={record.end_date}'
    return True


def setup_logger(logger, output, level=logging.INFO, cloud=True):
  # TODO:
  # logger.addFilter(ContextFilter())
  # formatter = logging.Formatter('[%(asctime)s] %(name)s [args:%(crawler_args)s] %(levelname)s: %(message)s')
  if logger.handlers:
    return

  formatter = logging.Formatter('[%(asctime)s] %(levelname)s %(name)s: %(message)s')

  file_handler = logging.FileHandler(f'logs/{output}')
  file_handler.setFormatter(formatter)
  if cloud:
    client = Client()
    cloud_handler = CloudLoggingHandler(client)
    destination =\
      'logging.googleapis.com/projects/inspira-development/locations/us-east1/buckets/log-crawlers'
    client.sink('log-crawlers', destination=destination)
    cloud_handler.setFormatter(formatter)
    logger.addHandler(cloud_handler)

  logger.addHandler(file_handler)
  logger.setLevel(level)


logging.getLogger('urllib3').setLevel(logging.WARNING)


def logger_factory(name, level=logging.INFO, output='crawlers.log'):
  logger = logging.getLogger(name)
  setup_logger(logger, output=output, level=level)
  return logger