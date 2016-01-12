'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''
from libcpp.vector cimport vector
from libcpp.string cimport string
from libc.stdio cimport printf
from .python_printer_callback import print_callback

cdef void print_status(string status_string) nogil:
    with gil:
        status_string = status_string.rstrip()
        print_callback(status_string)


cdef class PyCommClient:

    def __cinit__(self, vector[string] zkhosts, string name, unsigned int max_init_ping_failures=3,
                        public_key="", secret_key="", server_public_key="", ops_interruptible=True):
        self.thisptr = new comm_client(zkhosts, name, max_init_ping_failures, b"", b"", public_key,
                                                secret_key, server_public_key, ops_interruptible)
        assert (self.thisptr != NULL), "Fail to create comm client with zkhosts: %s, and host %s" % (str(zkhosts, name))
        self.thisptr.add_status_watch(b"PROGRESS", print_status)

    def __dealloc__(self):
        pass

    cpdef set_server_alive_watch_pid(self, int pid):
        self.thisptr.set_server_alive_watch_pid(pid)

    cpdef set_log_progress(self, bint is_on):
        if (is_on):
            self.thisptr.add_status_watch(b"PROGRESS", print_status)
        else:
            self.thisptr.remove_status_watch(b"PROGRESS")

    cpdef add_auth_method_token(self, string authtoken):
        assert self.thisptr != NULL
        self.thisptr.add_auth_method_token(authtoken)

    cpdef stop(self):
        assert self.thisptr != NULL
        with nogil:
            self.thisptr.stop()

    cpdef start(self):
        assert self.thisptr != NULL
        cdef reply_status r = self.thisptr.start()
        if (<size_t>r != 0):
            raise RuntimeError(reply_status_to_string(r))

def get_public_secret_key_pair():
    cdef char public_key[41]
    cdef char seceret_key[41]
    zmq_curve_keypair(public_key, seceret_key)
    return (str(public_key), str(seceret_key))
