"""
This module contains the main logic for start, query, stop graphlab server client connection.
"""

'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''

from ..cython.cy_unity import UnityGlobalProxy
from ..cython.cy_ipc import PyCommClient as Client
from ..cython.cy_ipc import get_public_secret_key_pair
from ..connect.server import LocalServer, RemoteServer
from ..connect import __SERVER__, __CLIENT__, _get_metric_tracker

import decorator
import logging
import os

""" The module level logger object """
__LOGGER__ = logging.getLogger(__name__)

LOCAL_SERVER_TYPE = 'local'
REMOTE_SERVER_TYPE = 'remote'

ENGINE_START_ERROR_MESSAGE = 'Cannot connect to GraphLab Create engine. ' + \
    'Contact support@dato.com for help.'

# Decorator which catch the exception and output to log error.
@decorator.decorator
def __catch_and_log__(func, *args, **kargs):
        try:
            return func(*args, **kargs)
        except Exception, error:
            logging.getLogger(__name__).error(error)


@__catch_and_log__
def launch(server_addr=None, server_bin=None, server_log=None, auth_token=None,
           server_public_key=''):
    """
    Launch a connection to the graphlab server. The connection can be stopped by
    the `stop` function.

    Automatically spawns a local server, if no arguments provided or "server_bin"
    is specified.

    Notes
    -----
        Only a single connection can exist at anytime.
        Prints warning if trying to launch while there is an active connection.

    Parameters
    ----------
    server_addr : string
        The address of the server.

    server_bin : string
        The path to the server binary (local server only).

    server_log : string
        The path to the server log (local server only).

    server_public_key : string
        The server's libsodium public key, used for encryption. Default is no encryption. 
    """
    if is_connected():
        __LOGGER__.warning(
            "Attempt to connect to a new server while still connected to a server."
            " Please stop the connection first by running 'graphlab.stop()' and try again.")
        return

    try:
        server_type = _get_server_type(server_addr)
        __LOGGER__.debug("Server type: %s" % server_type)
    except ValueError as e:
        __LOGGER__.error(e)
        _get_metric_tracker().track('server_launch.server_type_error', send_sys_info=True)
        return

    # construct a server server instance based on the server_type
    if (server_type == LOCAL_SERVER_TYPE):
        server = LocalServer(server_addr, server_bin, server_log)
    elif (server_type == REMOTE_SERVER_TYPE):
        server = RemoteServer(server_addr, auth_token, public_key=server_public_key)
    else:
        raise ValueError('Invalid server type: %s' % server_type)

    # start the server
    try:
        server.start()
    except Exception as e:
        __LOGGER__.error('Cannot start server: %s' % e)
        server.try_stop()
        raise

    # start the client
    (public_key, secret_key) = ('', '')
    if server_public_key != '':
       (public_key, secret_key) = get_public_secret_key_pair()
    try:
        num_tolerable_ping_failures = 4294967295
        client = Client([], server.get_server_addr(), num_tolerable_ping_failures,
                        public_key=public_key, secret_key=secret_key,
                        server_public_key=server_public_key)
        if hasattr(server, 'proc') and hasattr(server.proc, 'pid'):
            client.set_server_alive_watch_pid(server.proc.pid)
        if(auth_token is not None):
            client.add_auth_method_token(auth_token)
        client.start()
    except Exception as e:
        __LOGGER__.error("Cannot start client: %s" % e)
        if (client):
            client.stop()
        return

    _assign_server_and_client(server, client)

    assert is_connected()

def _get_server_type(server_addr):
    """
    Returns the server type in one of the {LOCAL_SERVER_TYPE, REMOTE_SERVER_TYPE},
    identified from the server_addr.

    Parameters
    ----------
    server_addr : string
        The server address url string.

    Returns
    --------
    out : server_type
        {'local', 'remote'}

    Raises
    -------
    ValueError
        on invalid server address.
    """
    # construct the server object
    # Depending on what are the parameters provided, decide to either
    # start a remote server or a local server
    if server_addr is None:
        server_type = LOCAL_SERVER_TYPE
    elif server_addr.startswith('tcp'):
        server_type = REMOTE_SERVER_TYPE
    elif server_addr.startswith('ipc'):
        if (os.path.exists(server_addr[6:])):
            server_type = REMOTE_SERVER_TYPE
        else:
            server_type = LOCAL_SERVER_TYPE
    else:
        raise ValueError('Invalid server address: %s. Server address must starts with ipc:// or tcp://.' % server_addr)
    return server_type


@__catch_and_log__
def stop():
    """
    Stops the current connection to the graphlab server.
    All object created to the server will be inaccessible.

    Reset global server and client object to None.
    """
    global __CLIENT__, __SERVER__
    if not is_connected():
        return
    __LOGGER__.info("Stopping the server connection.")
    if (__CLIENT__):
        __CLIENT__.stop()
        __CLIENT__ = None
    if (__SERVER__):
        __SERVER__.try_stop()
        __SERVER__ = None


def is_connected():
    """
    Returns true if connected to the server.
    """
    if (__CLIENT__ is not None and __SERVER__ is not None):
        # both client and server are live
        return True
    elif (__CLIENT__ is None and __SERVER__ is None):
        # both client and server are dead
        return False
    else:
        # unlikely state: one of them are live and the other dead
        raise RuntimeError('GraphLab connection error.')


def get_client():
    """
    Returns the global ipc client object, or None if no connection is present.
    """
    if not is_connected():
        launch()
    assert is_connected(), ENGINE_START_ERROR_MESSAGE
    return __CLIENT__


def get_server():
    """
    Returns the global graphlab server object, or None if no connection is present.
    """
    if not is_connected():
        launch()
    assert is_connected(), ENGINE_START_ERROR_MESSAGE
    return __SERVER__


def get_unity():
    """
    Returns the unity global object of the current connection.
    If no connection is present, automatically launch a localserver connection.
    """
    if not is_connected():
        launch()
    assert is_connected(), ENGINE_START_ERROR_MESSAGE
    return __UNITY_GLOBAL_PROXY__


def _assign_server_and_client(server, client):
    """
    Helper function to assign the global __SERVER__ and __CLIENT__ pair.
    """
    global __SERVER__, __CLIENT__, __UNITY_GLOBAL_PROXY__
    __SERVER__ = server
    __CLIENT__ = client
    __UNITY_GLOBAL_PROXY__ = UnityGlobalProxy(__CLIENT__)
    server.get_logger().info('GraphLab Server Version: %s' %
                             UnityGlobalProxy(client).get_version())

    from ..extensions import _publish
    _publish()



# Register an exit callback handler to stop the server on python exit.
import atexit
atexit.register(stop)
