'''
Copyright (C) 2016 Turi
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''
from .config import DEFAULT_CONFIG as CONFIG
from .metric_mock import MetricMock
from .sys_info import get_distinct_id, get_sys_info, get_version, get_isgpu, get_build_number


import logging
import pprint
import threading
import copy as _copy
import requests as _requests

import sys as _sys
if _sys.version_info.major == 3:
    import queue as Queue
    from urllib.parse import quote_plus as _quote_plus
else:
    import Queue
    from urllib import quote_plus as _quote_plus

__ALL__ = [ 'MetricTracker' ]


# global objects for producer/consumer for background metrics publishing
METRICS_QUEUE = Queue.Queue(maxsize=100)
METRICS_THREAD = None

SHUTDOWN_MESSAGE = 'SHUTTING_DOWN'


class _MetricsWorkerThread(threading.Thread):
  """Worker Thread for publishing metrics in the background."""

  def __init__(self, mode, source):
    threading.Thread.__init__(self, name='metrics-worker')
    # version and is_gpu from version_info
    self._version = get_version()
    self._build_number = get_build_number()
    self._isgpu = get_isgpu()

    self._mode = mode
    self._source = source
    try:
      # product key
      from .. import product_key
      self._product_key = product_key.get_product_key()
    except Exception as e:
      self._product_key = None

    self.queue = METRICS_QUEUE
    root_package_name = __import__(__name__.split('.')[0]).__name__
    self.logger = logging.getLogger(root_package_name + '.metrics')

    buffer_size = 5
    offline_buffer_size = 25
    self._sys_info_set = False

    self._usable = False
    try:

      self._metrics_url = CONFIG.metrics_url
      self._requests = _requests # support mocking out requests library in unit-tests

    except Exception as e:
      self.logger.warning("Unexpected exception connecting to Metrics service, disabling metrics, exception %s" % e)
    else:
      self._usable = True

    self._distinct_id = 'unknown'
    self._distinct_id = get_distinct_id(self._distinct_id)


  def run(self):
    while True:
      try:
        metric = self.queue.get() # block until something received

        if (metric['event_name'] == SHUTDOWN_MESSAGE):
          # shutting down
          self.queue.task_done()
          break

        self._track(metric['event_name'], metric['value'], metric['type'], metric['properties'], metric['meta'], metric['send_sys_info'])
        self.queue.task_done()

      except Exception as e:
        pass

  def _set_sys_info(self):
    # Don't do this if system info has been set
    if self._sys_info_set:
      return
    self._sys_info = get_sys_info()
    self._sys_info_set = True

  def _print_sys_info(self):
    pp = pprint.PrettyPrinter(indent=2)
    pp.pprint(self._sys_info)

  def _track(self, event_name, value=1, type="gauge", properties={}, meta={}, send_sys_info=False):
    """
    Internal method to actually send metrics, expected to be called from background thread only.
    """
    return
    if not self._usable:
      return
    the_properties = {}

    if send_sys_info:
      if not self._sys_info_set:
        self._set_sys_info()
      the_properties.update(self._sys_info)

    the_properties.update(properties)

    try:
      # homebrew metrics - cloudfront
      if self._metrics_url != '':
        cloudfront_props = {}
        props = _copy.deepcopy(the_properties)
        props.update(meta)

        cloudfront_props['event_name'] = event_name
        cloudfront_props['value'] = value
        cloudfront_props['distinct_id'] = self._distinct_id
        cloudfront_props['version'] = self._version
        cloudfront_props['isgpu'] = self._isgpu
        cloudfront_props['build_number'] = self._build_number
        cloudfront_props['properties'] = _quote_plus(str(props))

        # if product key is not set, then try to get it now when submitting
        if not self._product_key:
          try:
            # product key
            from .. import product_key
            self._product_key = product_key.get_product_key()
          except Exception as e:
            self._product_key = 'Unknown'
            pass

        cloudfront_props['product_key'] = self._product_key

        # self.logger.debug("SENDING '%s' to %s" % (cloudfront_props, self._metrics_url))
        logging.getLogger('requests').setLevel(logging.CRITICAL)
        self._requests.get(self._metrics_url, params=cloudfront_props)
    except Exception as e:
      pass


class MetricTracker:
  def __init__(self, mode='UNIT', background_thread=True):
    # setup logging
    root_package_name = __import__(__name__.split('.')[0]).__name__
    self.logger = logging.getLogger(root_package_name + '.metrics')

    self._mode = mode

    self._queue = METRICS_QUEUE

    self._source = ("%s-%s" % (self._mode, get_version()))
    self.logger.debug("Running with metric source: %s" % self._source)

    # background thread for metrics
    self._thread = None
    if background_thread:
      self._start_queue_thread()

  def __del__(self):
    try:
      self._stop_queue_thread()
    except:
      # Lot of strange exceptions can happen when destructing, not really anything we can do...
      pass

  def _stop_queue_thread(self):
    # send the shutting down message, wait for thread to exit
    if self._thread is not None:
      self.track(SHUTDOWN_MESSAGE)
      self._thread.join(2.0)


  def track(self, event_name, value=1, type="gauge", properties={}, meta={}, send_sys_info=False):
    """
    Publishes event / metric to metrics providers.

    This method is a facade / proxy, queuing up this metric for a background thread to process.
    """
    return
    if self._mode != 'PROD' and (not (isinstance(value, int) or isinstance(value, float))):
      raise Exception("Metrics attempted with value being not a number, unsupported.")

    try:
      item = dict(event_name=event_name, value=value, type=type, properties=properties, meta=meta, send_sys_info=send_sys_info)
      self._queue.put_nowait(item) # don't wait if Queue is full, just silently ignore
    except Queue.Full:
      if not self._thread or not self._thread.is_alive():
        self.logger.debug("Queue is full and background thread is no longer alive, trying to restart")
        self._restart_queue_thread()
      else:
        self.logger.debug("Queue is full, doing nothing.")

    except Exception as e:
      self.logger.debug("Unexpected exception in queueing metrics, %s" % e)

  def _start_queue_thread(self):
    global METRICS_THREAD
    if (self._thread is None):
      self.logger.debug("Starting background thread")
      self._thread = _MetricsWorkerThread(self._mode, self._source)
      METRICS_THREAD = self._thread
      self._thread.daemon = True
      self._thread.start()

  def _restart_queue_thread(self):
    global METRICS_THREAD

    if (self._thread is not None and self._thread.is_alive()):
      self._stop_queue_thread()

    METRICS_THREAD = None
    del self._thread
    self._thread = None

    self._start_queue_thread()
