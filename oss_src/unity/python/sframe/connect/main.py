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
from ..cython import cy_ipc
from ..connect.server import EmbededServer
from ..connect import __SERVER__, __CLIENT__

import decorator
import logging

""" The module level logger object """
__LOGGER__ = logging.getLogger(__name__)

""" Global varialbes """
__UNITY_GLOBAL_PROXY__ = None

ENGINE_START_ERROR_MESSAGE = 'Cannot connect to SFrame engine. ' + \
    'If you believe this to be a bug, check https://github.com/dato-code/SFrame/issues for known issues.'


# Decorator which catch the exception and output to log error.
@decorator.decorator
def __catch_and_log__(func, *args, **kargs):
        try:
            return func(*args, **kargs)
        except Exception, error:
            logging.getLogger(__name__).error(error)


@__catch_and_log__
def launch(server_addr=None, server_bin=None,
           server_log=None, auth_token=None, server_public_key=''):
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

    if server_addr is None:
        server_addr = 'inproc://sframe_server'

    # construct the server instance
    server = EmbededServer(server_addr, server_log)

    # start the server
    try:
        server.start()
    except Exception as e:
        __LOGGER__.error('Cannot start server: %s' % e)
        server.try_stop()
        return

    # start the client
    client = cy_ipc.make_comm_client_from_existing_ptr(server.get_client_ptr())

    _assign_server_and_client(server, client)

    assert is_connected()


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
    server.get_logger().info('SFrame Version: %s' %
                             UnityGlobalProxy(client).get_version())

    from ..extensions import _publish
    _publish()


# Register an exit callback handler to stop the server on python exit.
import atexit
atexit.register(stop)
