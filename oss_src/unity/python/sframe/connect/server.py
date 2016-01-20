"""
This module contains the interface for graphlab server, and the
implementation of a local graphlab server.
"""

'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''

from ..util.config import DEFAULT_CONFIG as default_local_conf
from ..connect import _get_metric_tracker
from .. import sys_util as _sys_util
import logging
import os
import sys
from ctypes import *


class GraphLabServer(object):
    """
    Interface class for a graphlab server.
    """
    def __init__(self):
        raise NotImplementedError

    def get_server_addr(self):
        """ Return the server address. """
        raise NotImplementedError

    def start(self, num_tolerable_ping_failures):
        """ Starts the server. """
        raise NotImplementedError

    def stop(self):
        """ Stops the server. """
        raise NotImplementedError

    def try_stop(self):
        """ Try stopping the server and swallow the exception. """
        try:
            self.stop()
        except:
            e = sys.exc_info()[0]
            self.get_logger().warning(e)

    def get_logger(self):
        """ Return the logger object. """
        raise NotImplementedError


class EmbeddedServer(GraphLabServer):
    """
    Embedded Server loads unity_server into the same process as shared library.
    """

    SERVER_LIB = 'libunity_server.%s' % _sys_util.get_current_platform_dll_extension()

    def __init__(self, server_address, unity_log_file):
        """
        @param unity_log_file string The path to the server logfile.
        """
        self.server_addr = server_address
        self.unity_log = unity_log_file
        self.logger = logging.getLogger(__name__)

        root_path = os.path.dirname(os.path.abspath(__file__))  # sframe/connect
        root_path = os.path.abspath(os.path.join(root_path, os.pardir))  # sframe/
        self.root_path = root_path
        self.started = False

        if not self.unity_log:
            self.unity_log = default_local_conf.get_unity_log()

        (success, dll_error_message) = self._load_dll_ok(self.root_path)
        if not success:
            raise RuntimeError("Cannot load library. %s" % dll_error_message)

    def __del__(self):
        self.stop()

    def get_server_addr(self):
        return self.server_addr

    def start(self):
        _get_metric_tracker().track('engine-started', value=1, send_sys_info=True)
        _get_metric_tracker().track('engine-started-local', value=1, send_sys_info=True)

        if sys.platform == 'win32':
            self.unity_log += ".0"

        try:
            self.dll.start_server(self.root_path, self.server_addr, self.unity_log,
                                  default_local_conf.log_rotation_interval,
                                  default_local_conf.log_rotation_truncate)
        except Exception as e:
            _get_metric_tracker().track('server_launch.unity_server_error', send_sys_info=True)
            raise RuntimeError("Cannot start engine. %s" % str(e))

        self.started = True

    def get_client_ptr(self):
        """
        Embedded server automatically constructs a client object
        Call this function to get pointer to a ready to use client
        """
        self.dll.get_client.restype = c_void_p
        return self.dll.get_client()

    def stop(self):
        if self.started:
            self.dll.stop_server()
            self.started = False

    def get_logger(self):
        return self.logger

    def _load_dll_ok(self, root_path):
        server_env = _sys_util.make_unity_server_env()
        os.environ.update(server_env)
        for (k, v) in server_env.iteritems():
            os.putenv(k, v)

        # For Windows, add path to DLLs for the pylambda_worker
        if sys.platform == 'win32':
            _sys_util.set_windows_dll_path()

        try:
            self.dll = CDLL(os.path.join(root_path, self.SERVER_LIB))
        except Exception as e:
            return (False, str(e))

        # Touch all symbols and make sure they exist
        try:
            self.dll.start_server.argtypes = [c_char_p, c_char_p, c_char_p, c_ulonglong, c_ulonglong]
            self.dll.get_client.restype = c_void_p
            self.dll.stop_server
        except Exception as e:
            return (False, str(e))

        return (True, "")
