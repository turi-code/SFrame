'''
Copyright (C) 2016 Turi
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''
import logging
import pprint

class ConsumerMock:
  def __init__(self):
    self._num_flushes = 0

  def flush(self):
    self._num_flushes += 1


class MetricMock:
  def __init__(self, event_limit=-1):
    self._consumer = ConsumerMock()
    self._calls = []
    self._pp = pprint.PrettyPrinter(indent=2)
    self._event_limit = event_limit
    self.logger = logging.getLogger(__name__)

  def track(self, distinct_id, event_name, properties={}, meta={}):
    if self._event_limit < 0 or len(_calls) < self._event_limit:
      self._calls.append( {'method':'track',
                      'distinct_id':distinct_id, 
                      'event_name':event_name,
                      'properties':properties,
                      'meta':meta,
                     })

  def submit(self, name, value, type, source, attributes):
    self._calls.append({'method':'submit',
                        'name':name,
                        'value':value,
                        'type':type,
                        'source':source,
                        'attributes':attributes})

  def dump_calls(self):
    #self._pp.pprint(self._calls)
    self.logger.info(self._calls)

  def dump(self):
    self.logger.info("Number of flushes: %g" % self._consumer._num_flushes)
    self.dump_calls()
