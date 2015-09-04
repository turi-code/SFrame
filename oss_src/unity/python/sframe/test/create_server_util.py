'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''
from ..connect import server
import os
import random
import time

def create_server(server_addr, auth_token=None, public_key='', secret_key=''):
    """
    Creates a server process, which listens on the server_addr, and uses
    auth_token. This function can be used for testing scenrios for remote launches, e.g.:

    >>> server = create_server('tcp://127.0.0.1:10000')
    >>> graphlab.launch('ipc://127.0.0.1:10000')
    >>> assert graphlab.connect.main.is_connected()

    Returns the LocalServer object which manages the server process.
    """
    server_bin = os.getenv("GRAPHLAB_UNITY")
    ts = str(int(time.time()))
    unity_log = "/tmp/test_connect_%s.log" % ts
    return server.LocalServer(server_addr, server_bin, unity_log, auth_token=auth_token,
                              public_key=public_key, secret_key=secret_key)


def start_test_tcp_server(auth_token=None, public_key='', secret_key='', failure_expected=False):
    MAX_NUM_PORT_ATTEMPTS = 10
    if failure_expected:
        MAX_NUM_PORT_ATTEMPTS = 1
    num_attempted_ports = 0
    server = None

    while(num_attempted_ports < MAX_NUM_PORT_ATTEMPTS):
        port = random.randint(8000, 65535)
        tcp_addr = 'tcp://127.0.0.1:%d' % port
        server = create_server(tcp_addr, auth_token, public_key=public_key, secret_key=secret_key)
        num_attempted_ports += 1

        if not failure_expected:
            try:
                server.start()
            except:
                # Occasionally we pick a port that's already in use.
                server.try_stop()
            else:
                break  # Success, port was not in use.

    assert(num_attempted_ports <= MAX_NUM_PORT_ATTEMPTS)
    return server

