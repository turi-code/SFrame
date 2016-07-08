''' Copyright (C) 2016 Turi All rights reserved.
This software may be modified and distributed under the terms of the BSD license. See the LICENSE file for details. '''

from unittest import TestCase
import logging
from .. import util as glutil


class LoggingConfigurationTests(TestCase):

    def setUp(self):
        """
        Cleanup the existing log configuration.
        """
        self.logger = logging.getLogger(glutil.root_package_name)
        self.orig_handlers = self.logger.handlers
        self.logger.handlers = []
        self.level = self.logger.level
        self.logger.level = logging.DEBUG

        self.rt_logger = logging.getLogger()
        self.orig_root_handlers = self.rt_logger.handlers
        self.rt_logger.handlers = []
        self.root_level = self.rt_logger.level
        self.rt_logger.level = logging.CRITICAL

    def tearDown(self):
        """
        Restore the original log configuration.
        """
        self.logger.handlers = self.orig_handlers
        self.logger.level = self.level
        self.rt_logger.handlers = self.orig_root_handlers
        self.rt_logger.level = self.root_level

    def test_config(self):
        glutil.init_logger()

        self.assertEquals(self.logger.level, logging.INFO)
        self.assertEquals(len(self.logger.handlers), 2)
        self.assertEquals(len(self.rt_logger.handlers), 0)

        self.assertEquals(self.logger.level, logging.INFO)
        self.assertEquals(self.rt_logger.level, logging.CRITICAL)
