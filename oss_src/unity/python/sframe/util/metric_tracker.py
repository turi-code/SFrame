'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''
from config import DEFAULT_CONFIG as CONFIG
from metric_mock import MetricMock

# metrics libraries
import mixpanel
import librato

import Queue
import logging
import os
import platform
import pprint
import threading
import uuid
import copy as _copy
import requests as _requests
import sys
import urllib as _urllib

__ALL__ = [ 'MetricTracker' ]

try:
  from .. import minipsutil
  TOTAL_PHYMEM = minipsutil.total_memory()
  NUM_CPUS = minipsutil.cpu_count()
except ImportError:
  TOTAL_PHYMEM = 0
  NUM_CPUS = 0

# global objects for producer/consumer for background metrics publishing
METRICS_QUEUE = Queue.Queue(maxsize=100)
METRICS_THREAD = None

SHUTDOWN_MESSAGE = 'SHUTTING_DOWN'


class _MetricsWorkerThread(threading.Thread):
  """Worker Thread for publishing metrics in the background."""

  def __init__(self, mode, source):
    threading.Thread.__init__(self, name='metrics-worker')

    if CONFIG.version.endswith('.gpu'):
        self._version = CONFIG.version.split('.gpu')[0]
        self._isgpu = True
    else:
        self._version = CONFIG.version
        self._isgpu = False

    self._mode = mode
    self._source = source
    try:
      # product key
      from .. import product_key
      self._product_key = product_key.get_product_key()
    except Exception, e:
      self._product_key = None

    self.queue = METRICS_QUEUE
    root_package_name = __import__(__name__.split('.')[0]).__name__
    self.logger = logging.getLogger(root_package_name + '.metrics')

    self._tracker = None # librato metrics tracker
    self._mixpanel = None # Mixpanel metrics tracker

    buffer_size = 5
    offline_buffer_size = 25
    self._sys_info_set = False

    self._usable = False
    try:

      self._metrics_url = CONFIG.metrics_url
      self._requests = _requests # support mocking out requests library in unit-tests

      if self._mode != 'PROD':
        self.logger.info("Using MetricMock instead of real metrics, mode is: %s" % self._mode)
        self._tracker = MetricMock()
        self._mixpanel = MetricMock()
      else:
        self._tracker = librato.connect(CONFIG.librato_user, CONFIG.librato_token)
        self._mixpanel = mixpanel.Mixpanel(CONFIG.mixpanel_user)
    except Exception, e:
      self.logger.warning("Unexpected exception connecting to Metrics service, disabling metrics, exception %s" % e)
    else:
      self._usable = True

    self._distinct_id = 'unknown'
    self._distinct_id = self._get_distinct_id()


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


  def _get_distinct_id(self):
    if self._distinct_id == 'unknown':
      poss_id = 'unknown'
      gldir = os.path.join(os.path.expanduser('~'),'.graphlab')
      try:
          if not os.path.isdir(gldir):
              os.makedirs(gldir)
      except:
          pass

      id_file_path = os.path.join(gldir, "id")

      if os.path.isfile(id_file_path):
        try:
          with open(id_file_path, 'r') as f:
            poss_id = f.readline()
        except:
          return "session-" + str(uuid.uuid4())
      else:
        # no distinct id found from installation,
        # try to create one and write it to the appropriate location
        # if not able to write to appropriate location, then create temp one
        new_id = str(uuid.uuid4())
        try:
          with open(id_file_path, "w") as id_file:
            id_file.write(new_id)
        except:
          return "session-" + str(uuid.uuid4())
        return new_id
      return poss_id.strip()
    else:
      return self._distinct_id


  @staticmethod
  def _get_bucket_name_suffix(buckets, value):
    """
    Given a list of buckets and a value, generate a suffix for the bucket
    name, corresponding to either one of the buckets given, or the largest
    bucket with "+" appended.
    """
    suffix = None
    for bucket in buckets:
        if value <= bucket:
            suffix = str(bucket)
            break
    # if we get here and suffix is None, value must be > the largest bucket
    if suffix is None:
        suffix = '%d+' % buckets[-1]
    return suffix

  @staticmethod
  def _bucketize_mixpanel(event_name, value):
    """
    Take events that we would like to bucketize and bucketize them before sending to mixpanel

    @param event_name current event name, used to assess if bucketization is required
    @param value value used to decide which bucket for event
    @return event_name if updated then will have bucket appended as suffix, otherwise original returned
    """

    if value == 1:
        return event_name

    bucket_events = {
        'col.size': [ 5, 10, 20 ],
        'row.size': [ 100000, 1000000, 10000000, 100000000 ],
        'duration.secs': [ 300, 1800, 3600, 7200 ],
        'duration.ms': [ 10, 100, 1000, 10000, 100000 ]
    }

    for (event_suffix, buckets) in bucket_events.iteritems():
        if event_name.endswith(event_suffix):
            # if the suffix matches one we expect, bucketize using the buckets defined above
            return '%s.%s' % (event_name, _MetricsWorkerThread._get_bucket_name_suffix(buckets, value))

    # if there was no suffix match, just use the original event name
    return event_name

  def _set_sys_info(self):
    # Don't do this if system info has been set
    if self._sys_info_set:
      return

    self._sys_info = {}

    # Get OS-specific info
    self._sys_info['system'] = platform.system()

    if self._sys_info['system'] == 'Linux':
      self._sys_info['os_version'] = self._tup_to_flat_str(platform.linux_distribution())
      self._sys_info['libc_version'] = self._tup_to_flat_str(platform.libc_ver())
    elif self._sys_info['system'] == 'Darwin':
      self._sys_info['os_version'] = self._tup_to_flat_str(platform.mac_ver())
    elif self._sys_info['system'] == 'Windows':
      self._sys_info['os_version'] = self._tup_to_flat_str(platform.win32_ver())
    elif self._sys_info['system'] == 'Java':
      self._sys_info['os_version'] = self._tup_to_flat_str(platform.java_ver())

    # Python specific stuff
    self._sys_info['python_implementation'] = platform.python_implementation()
    self._sys_info['python_version'] = platform.python_version()
    self._sys_info['python_build'] = self._tup_to_flat_str(platform.python_build())
    self._sys_info['python_executable'] = sys.executable

    # Dato specific stuff
    self._sys_info['dato_launcher'] = 'DATO_LAUNCHER' in os.environ

    # Get architecture info
    self._sys_info['architecture'] = self._tup_to_flat_str(platform.architecture())
    self._sys_info['platform'] = platform.platform()
    self._sys_info['num_cpus'] = NUM_CPUS

    # Get RAM size
    self._sys_info['total_mem'] = TOTAL_PHYMEM

    self._sys_info_set = True

  def _print_sys_info(self):
    pp = pprint.PrettyPrinter(indent=2)
    pp.pprint(self._sys_info)

  def _tup_to_flat_str(self, tup):
    tmp_list = []
    for t in tup:
      if isinstance(t, tuple):
        tmp_str =self._tup_to_flat_str(t)
        tmp_list.append(tmp_str)
      elif isinstance(t, str):
        tmp_list.append(t)
      else:
        # UNEXPECTED! Just don't crash
        try:
          tmp_list.append(str(t))
        except:
          pass
    return " ".join(tmp_list)

  def _send_mixpanel(self, event_name, value, properties, meta):
    # Only send 'engine-started' events to Mixpanel. All other events are not sent to Mixpanel.
    if 'engine-started' in event_name:
      try:
        # since mixpanel cannot send sizes or numbers, just tracks events, bucketize these here
        if value != 1:
          event_name = self._bucketize_mixpanel(event_name, value)
          properties['value'] = value
        properties['source'] = self._source
        self._mixpanel.track(self._distinct_id, event_name, properties=properties, meta=meta)
      except Exception as e:
        pass

  def _track(self, event_name, value=1, type="gauge", properties={}, meta={}, send_sys_info=False):
    """
    Internal method to actually send metrics, expected to be called from background thread only.
    """
    if not self._usable:
      return
    the_properties = {}

    if send_sys_info:
      if not self._sys_info_set:
        self._set_sys_info()
      the_properties.update(self._sys_info)

    the_properties.update(properties)

    try:
      # librato
      self._tracker.submit(name=event_name, value=value, type="gauge", source=self._source, attributes=the_properties)
    except Exception as e:
      pass

    self._send_mixpanel(event_name=event_name, value=value, properties=the_properties, meta=meta)

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
        cloudfront_props['properties'] = _urllib.quote_plus(str(props))

        # if product key is not set, then try to get it now when submitting
        if not self._product_key:
          try:
            # product key
            from .. import product_key
            self._product_key = product_key.get_product_key()
          except Exception, e:
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

    self._source = ("%s-%s" % (self._mode, CONFIG.version))
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
