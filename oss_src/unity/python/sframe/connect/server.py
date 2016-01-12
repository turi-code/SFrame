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

from ..cython import _encode
from ..cython.cy_ipc import PyCommClient as Client
from ..cython.cy_ipc import get_public_secret_key_pair
from ..util.config import DEFAULT_CONFIG as default_local_conf
from ..connect import _get_metric_tracker
from .. import sys_util as _sys_util
import logging
import os
import subprocess
import sys
import time


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


class LocalServer(GraphLabServer):
    """
    Local GraphLab Server wraps the graphlab
    unity_server process and implements the GraphLabServer interface.
    """

    __slot__ = ["server_bin", "server_addr", "unity_log", "proc", "auth_token"]

    def __init__(self, server_addr, server_bin, unity_log_file,
                 auth_token=None, public_key='', secret_key=''):
        """
        Constructs a new LocalServer

        @param server_bin string The path to the graphlab unity_server binary.

        @param server_addr string The address that the server is listening on. Server address must starts with 'ipc://' or 'tcp://'.

        @param unity_log_file string The path to the server logfile.

        @param public_key string The server's public encryption key.

        @param private_key string The server's private encryption key.
        """
        self.server_addr = server_addr
        self.server_bin = server_bin
        self.unity_log = unity_log_file
        self.auth_token = auth_token
        self.logger = logging.getLogger(__name__)
        self.proc = None
        self.public_key = public_key
        self.secret_key = secret_key

        # Either both or neither encryption keys must be set.
        assert(bool(public_key) == bool(secret_key))

        if not self.server_addr:
            # by default we use '/tmp/graphlab_server-$pid-$timestamp'
            # where the pid is the server process id
            # and timestamp is the current timestamp
            self.server_start_timestamp = int(time.time())
            self.server_addr = 'default-%d' % self.server_start_timestamp

        if not self.server_bin:
            if not default_local_conf.server_bin:
                raise RuntimeError("Unable to start local server. Please set the GRAPHLAB_UNITY environment variable.")
            else:
                self.server_bin = default_local_conf.server_bin

        if not self.unity_log:
            self.unity_log = default_local_conf.get_unity_log()

        self._validate_protocol_and_address()
        # check server binary
        if not os.path.exists(self.server_bin):
            raise ValueError('Invalid server binary \"%s\" does not exist.' % self.server_bin)
        if not os.access(self.server_bin, os.X_OK):
            raise ValueError('Invalid server binary \"%s\" is not executable. Please check your file permission.' % self.server_bin)

    def __del__(self):
        self.try_stop()

    def get_server_addr(self):
        return self.server_addr

    def _validate_protocol_and_address(self):
        # check server address
        if self.server_addr.startswith('default'):
            protocol = 'ipc'
        else:
            protocol = _get_server_addr_protocol(self.server_addr)
        if protocol not in ['ipc', 'tcp']:
            raise ValueError('Invalid server address: \"%s\". Addresses must start with ipc://' % self.server_addr)

    def start(self, num_tolerable_ping_failures=4294967295):
        _get_metric_tracker().track('engine-started', value=1, send_sys_info=True)
        _get_metric_tracker().track('engine-started-local', value=1)

        arglist = [self.server_bin, self.server_addr]

        if (self.auth_token):
            arglist.append("--auth_token=%s" % self.auth_token)

        if self.secret_key != '':
            arglist.append("--secret_key=%s" % self.secret_key)

        arglist.append("--log_file=%s" % self.unity_log)
        arglist.append("--log_rotation_interval=%d" % default_local_conf.log_rotation_interval)
        arglist.append("--log_rotation_truncate=%d" % default_local_conf.log_rotation_truncate)

        self._validate_protocol_and_address()

        if sys.platform == 'win32':
            self.unity_log += ".0"

        # Start a local server as a child process.
        try:
            FNULL = open(os.devnull, 'w')
            if sys.platform == 'win32':
                self.proc = subprocess.Popen(arglist,
                        env=_sys_util.make_unity_server_env(),
                        stdin=subprocess.PIPE, stdout=FNULL,
                        stderr=None, bufsize=-1) # preexec_fn not supported on windows
            else:
                self.proc = subprocess.Popen(arglist,
                        env=_sys_util.make_unity_server_env(),
                        stdin=subprocess.PIPE, stdout=FNULL,
                        stderr=None, bufsize=-1,
                        preexec_fn=lambda: os.setpgrp())  # do not forward signal
        except OSError as e:
            raise RuntimeError('Invalid server binary \"%s\": %s' % (self.server_bin, str(e)))
        except KeyError as e:
            raise RuntimeError(e.message)

        # update the default server_addr
        if (self.server_addr.startswith('default')):
            self.server_addr = 'ipc:///tmp/graphlab_server-%s-%d' % (self.proc.pid,
                                                                     self.server_start_timestamp)

        self.logger.info('Start server at: ' + self.server_addr + " - "
                         'Server binary: ' + self.server_bin + " - "
                         'Server log: ' + self.unity_log)

        # try to establish a connection to the server.
        (client_public_key, client_secret_key) = ('', '')
        if (self.public_key != '' and self.secret_key != ''):
            (client_public_key, client_secret_key) = get_public_secret_key_pair()

        max_retry = 5
        retry = 0
        server_alive = True
        c = None
        while retry < max_retry:
            retry += 1
            # Make sure the server process is still alive
            if (self.proc.poll() is not None):
                server_alive = False

            # OK, server is alive, try create a client and connect
            if (server_alive):
                try:
                    c = Client([], self.server_addr.encode(), num_tolerable_ping_failures,
                               public_key=client_public_key.encode(),
                               secret_key=client_secret_key.encode(),
                               server_public_key=self.public_key.encode())
                    if self.auth_token:
                        c.add_auth_method_token(_encode(self.auth_token))
                    c.set_server_alive_watch_pid(self.proc.pid)
                    c.start()
                    # everything works, break out of the retry loop
                    break
                except Exception as e:
                    self.logger.error('Try connecting to server. Error: %s. Retry = %d' % (str(e), retry))
                    time.sleep(0.5)
                finally:
                    if c:
                        c.stop()
            # Server process terminated, raise exception and get the return code
            else:
                retcode = self.proc.returncode
                self.proc = None
                self.logger.error('Cannot start server process. Return code: %d' % retcode)
                raise RuntimeError('Cannot start server process. Return code: %d' % retcode)

        if retry == max_retry:
            self.logger.error('Cannot connect to server. Exceeded max retry (%d).' % max_retry)
            raise RuntimeError('Cannot connect to server. Exceeded max retry (%d).' % max_retry)

        import threading

        def server_wait():
            self.proc.wait()

        self.wait_thread = threading.Thread(target=server_wait)
        self.wait_thread.setDaemon(True)
        self.wait_thread.start()

    def stop(self):
        num_polls_before_kill = 20
        if (self.proc):
            self.proc.communicate(b"\n")
            self.wait_thread.join()
            # Wait a couple of seconds for the process to die
            died = False
            for i in range(num_polls_before_kill):
                if self.proc.poll() is not None:
                    died = True
                    break
                time.sleep(0.1)
            # if I have not died in a couple of seconds
            # explicitly kill it
            if died is False:
                self.proc.kill()
            # join
            self.proc.wait()
            # ipc file. we should delete it since we are done
            if _get_server_addr_protocol(self.server_addr) is "ipc":
                filename = self.server_addr[6:]
                try:
                    os.remove(filename)
                except:
                    pass
            self.proc = None

    def get_logger(self):
        return self.logger


class RemoteServer(GraphLabServer):
    """
    The class which manages the connection to a remote unity_server.
    """
    def __init__(self, server_addr, auth_token=None, public_key=''):
        """
        Construct a new RemoteServer

        Parameters
        ----------
        server_addr : string
            The address that the server is listening on.

        auth_token : string
            The token to authroize the connection.

        public_key : string
            The public encryption key for the Remote Server.
        """
        self.server_addr = server_addr
        protocol = _get_server_addr_protocol(self.server_addr)
        if protocol not in ["tcp", "ipc"]:
            raise ValueError('Invalid remote server address: \"%s\".'
                             'Addresses must start with tcp:// or ipc://' % self.server_addr)
        self.logger = logging.getLogger(__name__)
        self.auth_token = auth_token
        self.public_key = public_key

    def get_server_addr(self):
        return self.server_addr

    def start(self, num_tolerable_ping_failures=3):
        _get_metric_tracker().track('engine-started', value=1, send_sys_info=True)
        _get_metric_tracker().track('engine-started-remote', value=1)

        # try to establish a connection to the server.
        (client_public_key, client_secret_key) = ('', '')
        if self.public_key != '':
            (client_public_key, client_secret_key) = get_public_secret_key_pair()
        try:
            c = Client([], self.server_addr.encode(), num_tolerable_ping_failures,
                       public_key=client_public_key.encode(), secret_key=client_secret_key.encode(),
                       server_public_key=self.public_key.encode())
            if self.auth_token:
                c.add_auth_method_token(_encode(self.auth_token))
            c.start()
        finally:
          c.stop()

    def stop(self):
        pass

    def get_logger(self):
        return self.logger


def _get_server_addr_protocol(addr):
    if len(addr) < 6 or "://" not in addr:
        raise ValueError("Invalid server address: %s" % addr)
    else:
        return addr.split("://")[0]
