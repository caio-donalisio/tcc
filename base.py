import concurrent.futures
import sys
import time
import utils
import logging
import pydantic
import json
import hashlib
import requests

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
  content : Any
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

  def setup(self):
    """Anything before execution"""
    pass

  def teardown(self):
    """Anything after execution"""
    pass

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


class HashedKeyValue:

  def __init__(self, keys : dict, prefix=None):
    self._keys   = keys
    self._state  = {}
    self._prefix = prefix

  @property
  def hash(self):
    return hashlib.sha1(repr(sorted(self._keys.items())).encode()) \
      .hexdigest()

  @property
  def prefix(self):
    return self._prefix

  @property
  def state(self):
    return self._state

  @property
  def keys(self):
    return self._keys

  def set_value(self, key, value):
    self._state[key] = value

  def get_value(self, key):
    return self._state[key]

  def add_value(self, key, value):
    if self._state.get(key) is None:
      self._state[key] = []
    self._state[key].append(value)

  def update_values(self, values : dict):
    self._state = {**self.state, **values}


class Chunk(HashedKeyValue):
  pass


class Snapshot(HashedKeyValue):
  pass


class HashedKeyValueRepository:

  def __init__(self, output, prefix):
    self._output = output
    self._prefix = prefix

  def restore(self, chunk):
    stored_values = json.loads(
        self._output.load_as_string(self.path_of(chunk)))
    chunk.update_values(stored_values)

  def exists(self, chunk):
    return self.commited(chunk)

  def commited(self, chunk):
    return self._output.exists(self.path_of(chunk))

  def commit(self, chunk):
    self._output.save_from_contents(
      filepath=self.path_of(chunk),
      contents=json.dumps(chunk.state),
      content_type='application/json')

  def path_of(self, chunk):
    if chunk._prefix:
      return f'{self._prefix}/{chunk._prefix}{chunk.hash}.state'
    else:
      return f'{self._prefix}/{chunk.hash}.state'


class ChunksRepository(HashedKeyValueRepository):
  def __init__(self, output):
    super(ChunksRepository, self).__init__(output, prefix='.state')


class SnapshotsRepository(HashedKeyValueRepository):
  def __init__(self, output):
    super(SnapshotsRepository, self).__init__(output, prefix='.snapshot')


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
  def __init__(self, executor, handler, repository : ChunksRepository):
    self.executor   = executor
    self.handler    = handler
    self.repository = repository

  def process(self, chunk) -> ChunkResult:
    if self.repository.commited(chunk):
      self.repository.restore(chunk)
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
    self.repository.commit(chunk)
    return ChunkResult(updates=records)

  def close(self):
    self.executor.shutdown()


class ChunkRunner:

  def __init__(self, collector : ICollector, processor : IChunkProcessor,
      repository : SnapshotsRepository, logger):
    self.collector  = collector
    self.processor  = processor
    self.repository = repository
    self.logger     = logger
    self.min_snapshot_interval = 30  # secs

  def run(self, snapshot : Snapshot = None):
    tqdm_out = utils.TqdmToLogger(self.logger, level=logging.INFO)

    hashmap = {}
    if snapshot and self.repository.exists(snapshot):
      self.repository.restore(snapshot)
      chunks   = snapshot.get_value('chunks')
      hashmap  = {chunk['hash']: chunk for chunk in chunks}

    try:
      self.collector.setup()

      last_snapshot_taken = 0
      expects = self.collector.count()
      records = 0

      with tqdm(total=expects, file=tqdm_out) as pbar:
        for chunk in self.collector.chunks():
          if chunk.hash in hashmap:
            chunk_records = hashmap[chunk.hash]['records']
            pbar.set_postfix(chunk.keys)
            pbar.update(chunk_records)
            records += chunk_records
            continue

          chunk_result = self.processor.process(chunk)
          pbar.set_postfix(chunk.keys)
          pbar.update(chunk_result.updates)
          records += chunk_result.updates

          if snapshot:
            snapshot.set_value('records', records)
            snapshot.set_value('expects', expects)
            snapshot.add_value('chunks' , {
              'hash': chunk.hash,
              'keys': chunk.keys,
              'records': chunk_result.updates,
              'state': chunk.state
            })

          if snapshot and \
            time.time() - last_snapshot_taken >= self.min_snapshot_interval:
            self.repository.commit(snapshot)
            last_snapshot_taken = time.time()
    finally:
      if snapshot:
        self.repository.commit(snapshot)
      self.processor.close()
      self.collector.teardown()

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
    if self.output.exists(event.dest):
      return

    response = requests.get(event.src,
      allow_redirects=True,
      verify=False)

    if response.status_code == 404:
      return

    if response.status_code == 200:
      self.output.save_from_contents(
        filepath=event.dest,
        contents=response.content,
        content_type=event.content_type)


def get_default_runner(collector, output, handler, logger, **kwargs):
  import concurrent.futures
  executor = concurrent.futures.ThreadPoolExecutor(max_workers=kwargs.get('max_workers', 2))

  chunks_repository = ChunksRepository(output=output)

  processor =\
    FutureChunkProcessor(executor=executor,
      handler=handler, repository=chunks_repository)

  snapshots_repository = SnapshotsRepository(output=output)
  runner = ChunkRunner(
    collector=collector,
    processor=processor,
    repository=snapshots_repository,
    logger=logger
  )

  return runner