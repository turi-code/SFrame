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


cdef class PyCommClient:

    def __cinit__(self):
        pass

    def __dealloc__(self):
        pass

    cpdef stop(self):
        assert self.thisptr != NULL
        self.thisptr.stop()

    cpdef start(self):
        assert self.thisptr != NULL
        cdef reply_status r = self.thisptr.start()
        if (<size_t>r != 0):
            raise RuntimeError(reply_status_to_string(r))

def make_comm_client(string name,
                     unsigned int max_init_ping_failures=3,
                     public_key="",
                     secret_key="",
                     server_public_key="",
                     ops_interruptible=True):
    cdef vector[string] dummy_zkhosts 
    client_ptr = new comm_client(dummy_zkhosts, name, max_init_ping_failures, "", "", public_key,
                                 secret_key, server_public_key, ops_interruptible)
    assert (client_ptr!= NULL), "Fail to create comm client with zkhosts: %s, and host %s" % (str(dummy_zkhosts, name))
    ret = PyCommClient()
    ret.thisptr = client_ptr
    return ret

def make_comm_client_from_existing_ptr(size_t client_ptr):
    ret = PyCommClient()
    ret.thisptr = <comm_client*>(client_ptr)
    return ret
