import concurrent.futures
import sys
import utils
import logging
import pydantic
from tqdm import tqdm
from bs4 import BeautifulSoup
from typing import Any


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
  content_type : str

  def __repr__(self):
    return f'Content(value={self.content[:10]}..., dest={self.dest})'


class ContentFromURL(pydantic.BaseModel):
  """Represents a content we should get from a url and save to destination url.
  """
  src : str
  dest : str
  content_type : str

  def __repr__(self):
    return f'ContentFromURL(src={self.src}..., dest={self.dest})'


class Late(pydantic.BaseModel):
  postpone : Any


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
              self.logger.debug(f"Chunk {chunk.hash} already commited ({chunk_records} records) -- skipping.")
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



class ICollector:

  def count(self) -> int:
    """Must return the number of records
    """
    raise Exception('Must be implemented in a subclass')

  def chunks(self):
    raise Exception('Must be implemented in a subclass')

  def countable(self):
    """Whether is countable -- whereis when we
    know beforehand the number of records
    """
    return True


import json
import hashlib

class Chunk:

  def __init__(self, keys : dict):
    self._keys  = keys
    self._state = {}

  @property
  def hash(self):
    return hashlib.sha1(repr(sorted(self._keys.items())).encode()) \
      .hexdigest()

  @property
  def state(self):\
    return self._state

  def set_value(self, key, value):
    self._state[key] = value

  def get_value(self, key):
    return self._state[key]

  def update_values(self, values : dict):
    self._state = {**self.state, **values}


class ChunkStateManager:

  def __init__(self, output):
    self._output = output

  def restore(self, chunk):
    stored_values = json.loads(
        self._output.load_as_string(self.path_of(chunk)))
    chunk.update_values(stored_values)

  def commited(self, chunk):
    return self._output.exists(self.path_of(chunk))

  def commit(self, chunk):
    self._output.save_from_contents(
      filepath=self.path_of(chunk),
      contents=json.dumps(chunk.state),
      content_type='application/json')

  def path_of(self, chunk):
    return f'.state/{chunk.hash}.state'


class ChunkResult:
  def __init__(self, updates):
    self._updates = updates

  @property
  def updates(self) -> int:
    return self._updates


class IChunkProcessor:

  def process(self, chunk) -> ChunkResult:
    raise Exception('Must be implemented in a subclass')


class FutureChunkProcessor(IChunkProcessor):
  def __init__(self, executor, handler, manager):
    self.executor = executor
    self.handler  = handler
    self.manager  = manager

  def process(self, chunk) -> ChunkResult:
    if self.manager.commited(chunk):
      self.manager.restore(chunk)
      return ChunkResult(updates=chunk.get_value('records'))

    futures = []
    records = 0
    for record in chunk.rows():
      records += 1
      for item in record:
        futures.append(self.executor.submit(
          self.handler.handle, item))

    for future in concurrent.futures.as_completed(futures):
      future.result()

    chunk.set_value('records', records)
    self.manager.commit(chunk)
    return ChunkResult(updates=records)


class ChunkRunner:

  def __init__(self, collector : ICollector, processor : IChunkProcessor):
    self.collector = collector
    self.processor = processor

  def run(self):
    with tqdm(total=self.collector.count()) as pbar:
      for chunk in self.collector.chunks():
        chunk_result = self.processor.process(chunk)
        pbar.update(chunk_result.updates)


class ContentHandler:

  def __init__(self, output):
    self.output = output

  def handle(self, event):
    if isinstance(event, Content):
      return self._handle_content_event(event)
    elif isinstance(event, ContentFromURL):
       return self._handle_url_event(event)

  def _handle_content_event(self, event : Content):
    return self.output.save_from_contents(
      filepath=event.dest,
      contents=event.content,
      content_type=event.content_type)

  def _handle_url_event(self, event : ContentFromURL):
    # Info: How allow client code do the request instead?
    import requests

    response = requests.get(event.src,
      allow_redirects=True,
      verify=False)

    if response.status_code == 404:
      return

    assert response.status_code == 200

    self.output.save_from_contents(
      filepath=event.dest,
      contents=response.content,
      content_type=event.content_type)
