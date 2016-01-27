# cython: c_string_type=str, c_string_encoding=utf8
'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''
from .cy_ipc cimport PyCommClient 
from .cy_variant cimport to_value 
from .cy_variant cimport from_dict as variant_map_from_dict

cdef create_model_from_proxy(PyCommClient cli, const model_base_ptr& proxy):
    if proxy.get() == NULL:
        return None
    ret = UnityModel()
    ret._cli = cli
    ret._base_ptr = proxy
    ret.thisptr = <model_proxy*>(ret._base_ptr.get())
    return ret


cdef class UnityModel: 
    cpdef list_fields(self):
        return self.thisptr.list_keys()

    cpdef get(self, string key, opts=None):
        cdef variant_map_type optional_args = variant_map_from_dict(opts)
        cdef variant_type value
        with nogil:
          value = self.thisptr.get_value(key, optional_args)
        return to_value(self._cli, value)
