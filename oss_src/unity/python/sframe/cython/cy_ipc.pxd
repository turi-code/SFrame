'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''
from libcpp.vector cimport vector
from libcpp.string cimport string 


cdef extern from "<cppipc/common/message_types.hpp>" namespace 'cppipc':
    cdef cppclass reply_status:
        pass

cdef extern from "<cppipc/client/comm_client.hpp>" namespace 'cppipc':
    cdef cppclass comm_client nogil:
        reply_status start() except + 
        void stop() except +

cdef extern from "cppipc/common/message_types.hpp" namespace 'cppipc':
    cdef string reply_status_to_string(reply_status)

cdef class PyCommClient:
    cdef comm_client *thisptr 
    cpdef stop(self)
    cpdef start(self)

cdef void print_status(string status_string) nogil

cpdef get_print_status_function_pointer()
