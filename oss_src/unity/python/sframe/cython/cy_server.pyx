"""
This module contains the interface for graphlab server, and the
implementation of a local graphlab server.

Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
"""

from ..util.config import DEFAULT_CONFIG as default_local_conf
from ..connect import _get_metric_tracker
from .. import sys_util as _sys_util
import logging
import os
import sys
from libcpp.string cimport string


cdef extern from "<unity/server/unity_server_capi.hpp>" namespace "graphlab":

    cdef cppclass unity_server_options:
        string server_address
        string control_address
        string publish_address
        string auth_token
        string secret_key
        string log_file
        string root_path
        bint daemon
        size_t log_rotation_interval
        size_t log_rotation_truncate

    void start_server(const unity_server_options& server_options)
    void* get_client()
    void stop_server()
    void set_log_progress "graphlab::unity_server::set_log_progress"(bint enable)
        
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

    def set_log_progress(self, enable):
        """ Enable or disable log progress printing """
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

    def __del__(self):
        self.stop()

    def get_server_addr(self):
        return self.server_addr

    def start(self):
        _get_metric_tracker().track('engine-started', value=1, send_sys_info=True)
        _get_metric_tracker().track('engine-started-local', value=1, send_sys_info=True)

        if sys.platform == 'win32':
            self.unity_log += ".0"

        # Set up the structure used to call it with all these parameters.
        cdef unity_server_options server_opts
        server_opts.root_path = self.root_path
        server_opts.server_address = self.server_addr
        server_opts.log_file = self.unity_log
        server_opts.log_rotation_interval = default_local_conf.log_rotation_interval
        server_opts.log_rotation_truncate = default_local_conf.log_rotation_truncate

        # Now, set up the environment.  TODO: move these in to actual
        # server parameters.
        server_env = _sys_util.make_unity_server_env()
        os.environ.update(server_env)
        for (k, v) in server_env.iteritems():
            os.putenv(k, v)

        # Try starting the server
        try:
            start_server(server_opts)
        except Exception as e:
            _get_metric_tracker().track('server_launch.unity_server_error', send_sys_info=True)
            raise

        self.started = True

    def get_client_ptr(self):
        """
        Embedded server automatically constructs a client object
        Call this function to get pointer to a ready to use client
        """
        return <size_t>(get_client())

    def stop(self):
        if self.started:
            stop_server()
            self.started = False

    def get_logger(self):
        return self.logger

    def set_log_progress(self, bint enable):
        set_log_progress(enable)
