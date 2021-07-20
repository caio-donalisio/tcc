import sys
import utils
import logging
import pydantic
from tqdm import tqdm
from bs4 import BeautifulSoup


class BaseCrawler:
  def __init__(self, params, output, logger, **options):
    self.output = output
    self.logger = logger
    self.params = params
    self.options = (options or {})

  def _current_soup(self):
    return BeautifulSoup(self.browser.page_source(), features='html.parser')

  def _find(self, matcher=None, **kwargs):
    return self._current_soup().find(matcher, **kwargs)

  def _find_all(self, matcher=None, **kwargs):
    return self._current_soup().find_all(matcher, **kwargs)


class Content(pydantic.BaseModel):
  """Represents a `saved` content to a destination uri
  """
  content : str
  dest : str

  def __repr__(self):
    return f'Content(value={self.content[:10]}..., dest={self.dest})'


class ContentFromURL(pydantic.BaseModel):
  """Represents a content we should get from a url and save to destination url.
  """
  src : str
  dest : str

  def __repr__(self):
    return f'ContentFromURL(src={self.src}..., dest={self.dest})'


class SkipWithBlock(Exception):
  pass


class ChunkTransaction:
  def __init__(self, chunk):
    self.chunk = chunk

  def __enter__(self):
    if self.chunk.commited():
      print('Chunk already commited')
      sys.settrace(lambda *args, **keys: None)
      frame = sys._getframe(1)
      frame.f_trace = self.trace

  def trace(self, frame, event, arg):
    raise SkipWithBlock()

  def __exit__(self, exc_type, exc_value, exc_traceback):
    if exc_type is None:
      self.chunk.commit()
      return  # No exception
    if issubclass(exc_type, SkipWithBlock):
      return True


class Runner:
  def __init__(self, chunks_generator, row_to_futures, total_records, logger, **options):
    self.chunks_generator  = chunks_generator
    self.row_to_futures    = row_to_futures
    self.total_records     = total_records
    self.logger            = logger
    self.options           = (options or {})

  def run(self):
    import concurrent.futures

    records_fetch = 0
    try:
      tqdm_out = utils.TqdmToLogger(self.logger, level=logging.INFO)
      with concurrent.futures.ThreadPoolExecutor(max_workers=self.options.get('max_workers', 2)) as executor:
        with tqdm(total=self.total_records, file=tqdm_out) as pbar:
          for chunk in self.chunks_generator:
            if chunk.commited():
              chunk_records  = chunk.get_value('records')
              records_fetch += chunk_records
              pbar.set_postfix(chunk.params)
              pbar.update(chunk_records)
              self.logger.info(f"Chunk {chunk.hash} already commited ({chunk_records} records) -- skipping.")
              continue

            chunk_records = 0
            futures = []
            for record in chunk.rows():
              chunk_records += 1
              for fn, args in self.row_to_futures(record):
                futures.append(executor.submit(fn, **args))

            for future in concurrent.futures.as_completed(futures):
              future.result()

            chunk.set_value('records', chunk_records)
            chunk.commit()
            records_fetch += chunk_records
            pbar.set_postfix(chunk.params)
            pbar.update(chunk_records)
            self.logger.debug(f'Chunk {chunk.hash} ({chunk_records} records) commited.')

    finally:
      pass
