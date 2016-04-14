'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''
from libcpp.vector cimport vector
from libcpp.string cimport string 
from libcpp.pair cimport pair
from .cy_ipc cimport PyCommClient
from .cy_unity_base_types cimport * 
from .cy_variant cimport variant_type, variant_map_type


cdef create_model_from_proxy(PyCommClient cli, const model_base_ptr& proxy)

cdef extern from "<unity/lib/api/model_interface.hpp>" namespace 'graphlab':
    cdef cppclass model_proxy nogil:
        vector[string] list_keys() except +
        variant_type get_value(string, variant_map_type&) except +

cdef class UnityModel:
    cdef model_proxy* thisptr
    cdef model_base_ptr _base_ptr
    cdef PyCommClient _cli

    cpdef list_fields(self)

    cpdef get(self, key, opts=*)
