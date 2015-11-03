'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''
# distutils: language = c++
import cython
from libcpp.string cimport string
from libcpp.pair cimport pair
cimport cy_graph
cimport cy_sframe
cimport cy_sarray
from .cy_graph cimport UnityGraphProxy
from .cy_model cimport create_model_from_proxy
from .cy_model cimport UnityModel
from .cy_sframe cimport UnitySFrameProxy
from .cy_sarray cimport UnitySArrayProxy

from .cy_flexible_type cimport flexible_type
from .cy_flexible_type cimport flexible_type_from_pyobject
from .cy_flexible_type cimport pyobject_from_flexible_type

from .cy_dataframe cimport gl_dataframe
from .cy_dataframe cimport is_pandas_dataframe
from .cy_dataframe cimport gl_dataframe_from_pd
from .cy_dataframe cimport pd_from_gl_dataframe

from cython.operator cimport dereference as deref
from cython.operator cimport preincrement as inc
from .cy_unity cimport is_function_closure_info
from .cy_unity cimport make_function_closure_info
from .cy_unity cimport variant_set_closure
from .cy_unity cimport function_closure_info

cdef bint variant_contains_flexible_type(const variant_type& v) except *:
    return v.which() == 0

cdef bint variant_contains_graph(const variant_type& v) except *:
    return v.which() == 1

cdef bint variant_contains_dataframe(const variant_type& v) except *:
    return v.which() == 2

cdef bint variant_contains_model(const variant_type& v) except *:
    return v.which() == 3

cdef bint variant_contains_sframe(const variant_type& v) except *:
    return v.which() == 4

cdef bint variant_contains_sarray(const variant_type& v) except *:
    return v.which() == 5

cdef bint variant_contains_variant_map(const variant_type& v) except *:
    return v.which() == 6

cdef bint variant_contains_variant_vector(const variant_type& v) except *:
    return v.which() == 7

cdef bint variant_contains_closure(const variant_type& v) except *:
    return v.which() == 8

cdef variant_vector_type from_list(object d) except *:
    cdef variant_vector_type ret
    if d:
        for v in d:
            ret.push_back(from_value(v))

    return ret
cdef variant_map_type from_dict(object d) except *:
    cdef variant_map_type ret
    if d:
        for key in d:
            v = d[key]
            ret[key] = from_value(v)

    return ret

cdef bint contains_top_level_variant_type(object v):
    if isinstance(v, UnityGraphProxy):
        return True
    elif str(type(v)) == "<class 'graphlab.data_structures.sgraph.SGraph'>":
        return True
    elif str(type(v)) == "<class 'sframe.data_structures.sgraph.SGraph'>":
        return True
    elif is_pandas_dataframe(v):
        return True
    elif isinstance(v, UnityModel):
        return True
    elif hasattr(v, '_tkclass') and isinstance(v._tkclass, UnityModel):
        return True
    elif str(type(v)) == "<class 'graphlab.data_structures.sframe.SFrame'>":
        return True
    elif str(type(v)) == "<class 'sframe.data_structures.sframe.SFrame'>":
        return True
    elif isinstance(v, UnitySFrameProxy):
        return True
    elif str(type(v)) == "<class 'graphlab.data_structures.sarray.SArray'>":
        return True
    elif str(type(v)) == "<class 'sframe.data_structures.sarray.SArray'>":
        return True
    elif isinstance(v, UnitySArrayProxy):
        return True
    elif isinstance(v, list):
        # ok... can the list be stored in a flexible_type?
        return any([contains_top_level_variant_type(i) for i in v])
    elif isinstance(v, dict):
        return any([contains_top_level_variant_type(i) for i in v.iterkeys()]) or \
                any([contains_top_level_variant_type(i) for i in v.itervalues()])
    elif is_function_closure_info(v):
        return True
    else:
        return False
    return False

cdef variant_type from_value(object v) except *:
    cdef variant_type ret 
    cdef flexible_type flex
    if isinstance(v, UnityGraphProxy):
        variant_set_graph(ret, (<UnityGraphProxy?>v)._base_ptr)
    elif str(type(v)) == "<class 'graphlab.data_structures.sgraph.SGraph'>":
        variant_set_graph(ret, (<UnityGraphProxy?>(v.__proxy__))._base_ptr)
    elif str(type(v)) == "<class 'sframe.data_structures.sgraph.SGraph'>":
        variant_set_graph(ret, (<UnityGraphProxy?>(v.__proxy__))._base_ptr)
    elif is_pandas_dataframe(v):
        variant_set_dataframe(ret, gl_dataframe_from_pd(v))
    elif isinstance(v, UnityModel):
        variant_set_model(ret, (<UnityModel?>v)._base_ptr)
    elif hasattr(v, '_tkclass') and isinstance(v._tkclass, UnityModel):
        variant_set_model(ret, (<UnityModel?>(v._tkclass))._base_ptr)
    elif str(type(v)) == "<class 'graphlab.data_structures.sframe.SFrame'>":
        variant_set_sframe(ret, (<UnitySFrameProxy?>(v.__proxy__))._base_ptr)
    elif str(type(v)) == "<class 'sframe.data_structures.sframe.SFrame'>":
        variant_set_sframe(ret, (<UnitySFrameProxy?>(v.__proxy__))._base_ptr)
    elif isinstance(v, UnitySFrameProxy):
        variant_set_sframe(ret, (<UnitySFrameProxy?>(v))._base_ptr)
    elif str(type(v)) == "<class 'graphlab.data_structures.sarray.SArray'>":
        variant_set_sarray(ret, (<UnitySArrayProxy?>(v.__proxy__))._base_ptr)
    elif str(type(v)) == "<class 'sframe.data_structures.sarray.SArray'>":
        variant_set_sarray(ret, (<UnitySArrayProxy?>(v.__proxy__))._base_ptr)
    elif isinstance(v, UnitySArrayProxy):
        variant_set_sarray(ret, (<UnitySArrayProxy?>(v))._base_ptr)
    elif isinstance(v, list):
        if contains_top_level_variant_type(v):
            variant_set_variant_vector(ret, from_list(v))
        else:
            flex = flexible_type_from_pyobject(v)
            variant_set_flexible_type(ret, flex)
    elif isinstance(v, dict):
        if contains_top_level_variant_type(v):
            variant_set_variant_map(ret, from_dict(v))
        else:
            flex = flexible_type_from_pyobject(v)
            variant_set_flexible_type(ret, flex)
    elif is_function_closure_info(v):
        variant_set_closure(ret, make_function_closure_info(v))
    else:
        variant_set_flexible_type(ret, flexible_type_from_pyobject(v))
    return ret

cdef to_dict(PyCommClient cli, variant_map_type& d):
    ret = {}
    cdef variant_map_type_iterator it
    it = d.begin()
    while (it != d.end()):
        ret[deref(it).first] = to_value(cli, deref(it).second)
        inc(it)
    return ret


cdef to_vector(PyCommClient cli, variant_vector_type& d):
    ret = []
    cdef variant_vector_type_iterator it
    it = d.begin()
    while (it != d.end()):
        ret += [to_value(cli, deref(it))]
        inc(it)
    return ret

cdef to_value(PyCommClient cli, variant_type& v):
        if (variant_contains_graph(v)):
            return cy_graph.create_proxy_wrapper_from_existing_proxy(
                cli, variant_get_graph(v))
        elif (variant_contains_dataframe(v)):
            return pd_from_gl_dataframe(variant_get_dataframe(v))
        elif (variant_contains_flexible_type(v)):
            return pyobject_from_flexible_type(variant_get_flexible_type(v))
        elif (variant_contains_model(v)):
            return create_model_from_proxy(cli, variant_get_model(v))
        elif (variant_contains_sframe(v)):
            return cy_sframe.create_proxy_wrapper_from_existing_proxy(
                cli, variant_get_sframe(v))
        elif (variant_contains_sarray(v)):
            return cy_sarray.create_proxy_wrapper_from_existing_proxy(
                cli, variant_get_sarray(v))
        elif (variant_contains_variant_map(v)):
            return to_dict(cli, variant_get_variant_map(v))
        elif (variant_contains_variant_vector(v)):
            return to_vector(cli, variant_get_variant_vector(v))
        else:
            raise TypeError("Unknown variant type.")
