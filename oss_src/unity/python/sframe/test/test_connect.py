'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''
import mock
import os
import tempfile
import unittest

from ..connect import main as glconnect
from .util import SubstringMatcher
root_package_name = __import__(__name__.split('.')[0]).__name__
class ConnectLocalTests(unittest.TestCase):
    def test_launch(self):
        #default launch
        glconnect.launch()
        self.assertTrue(glconnect.is_connected())
        glconnect.stop()
        self.assertFalse(glconnect.is_connected())

        #launch with server address
        tmpname = tempfile.NamedTemporaryFile().name
        tmpaddr = 'ipc://' + tmpname
        glconnect.launch(tmpaddr)
        self.assertTrue(glconnect.is_connected())
        glconnect.stop()
        self.assertFalse(glconnect.is_connected())
        #check that the ipc file gets deleted
        self.assertFalse(os.path.exists(tmpname))

        #launch address and binary
        graphlab_bin = os.getenv("GRAPHLAB_UNITY")
        glconnect.launch(server_addr=tmpaddr,
                         server_bin=graphlab_bin)
        self.assertTrue(glconnect.is_connected())
        glconnect.stop()
        self.assertFalse(glconnect.is_connected())
        self.assertFalse(os.path.exists(tmpname))

    @mock.patch(root_package_name + '.connect.main.__LOGGER__')
    def test_launch_with_exception(self, mock_logging):
        # Assert warning logged when launching without stopping
        glconnect.launch()
        glconnect.launch()
        self.assertTrue(mock_logging.warning.called_once_with(SubstringMatcher(containing="existing server")))
        self.assertTrue(glconnect.is_connected())
        glconnect.stop()
        self.assertFalse(glconnect.is_connected())

        # launch with bogus server binary (path is not executable)
        with tempfile.NamedTemporaryFile() as f:
            random_server_bin = f.name
            glconnect.launch(server_bin=random_server_bin)
            self.assertTrue(mock_logging.error.called_once_with(SubstringMatcher(containing="Invalid server binary")))
            self.assertFalse(glconnect.is_connected())

        # launch with binary that does not exist
        tmpname = tempfile.NamedTemporaryFile().name
        glconnect.launch(server_bin=tmpname)
        self.assertTrue(mock_logging.error.called_once_with(SubstringMatcher(containing="Invalid server binary")))
        self.assertFalse(glconnect.is_connected())
