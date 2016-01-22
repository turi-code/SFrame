'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''
# distutils: language = c++
import cython
from .cy_ipc cimport PyCommClient
from .cy_dataframe cimport gl_dataframe
from .cy_dataframe cimport dataframe_t
from .cy_flexible_type cimport flexible_type
from .cy_unity_base_types cimport *
from libcpp.map cimport map
from libcpp.vector cimport vector
from libcpp.string cimport string

cdef extern from "<unity/lib/api/client_base_types.hpp>" namespace "graphlab":
    cdef cppclass variant_type:
        variant_type()
        int which()
        variant_type& set_flexible_type "operator=<graphlab::flexible_type>"(const flexible_type& other)
        variant_type& set_graph "operator=<std::shared_ptr<graphlab::unity_sgraph_base>>"(const unity_sgraph_base_ptr& other)
        variant_type& set_dataframe "operator=<graphlab::dataframe_t>"(const gl_dataframe& other)
        variant_type& set_model "operator=<std::shared_ptr<graphlab::model_base>>"(const model_base_ptr& other)
        variant_type& set_sframe "operator=<std::shared_ptr<graphlab::unity_sframe_base>>"(const unity_sframe_base_ptr& other)
        variant_type& set_sarray "operator=<std::shared_ptr<graphlab::unity_sarray_base>>"(const unity_sarray_base_ptr& other)


    flexible_type& variant_get_flexible_type "graphlab::variant_get_ref<graphlab::flexible_type>" (variant_type& v)
    gl_dataframe& variant_get_dataframe "graphlab::variant_get_ref<graphlab::dataframe_t>" (variant_type& v)
    unity_sgraph_base_ptr& variant_get_graph "graphlab::variant_get_ref<std::shared_ptr<graphlab::unity_sgraph_base>>" (variant_type& v)
    model_base_ptr& variant_get_model "graphlab::variant_get_ref<std::shared_ptr<graphlab::model_base>>" (variant_type& v)
    unity_sframe_base_ptr& variant_get_sframe "graphlab::variant_get_ref<std::shared_ptr<graphlab::unity_sframe_base>>" (variant_type& v)
    unity_sarray_base_ptr& variant_get_sarray "graphlab::variant_get_ref<std::shared_ptr<graphlab::unity_sarray_base>>" (variant_type& v)
    map[string, variant_type]& variant_get_variant_map "graphlab::variant_get_ref<graphlab::variant_map_type>" (variant_type& v)
    vector[variant_type]& variant_get_variant_vector "graphlab::variant_get_ref<graphlab::variant_vector_type>" (variant_type& v)

    const flexible_type& variant_get_flexible_type "graphlab::variant_get_ref<graphlab::flexible_type>" (const variant_type& v)
    const gl_dataframe& variant_get_dataframe "graphlab::variant_get_ref<graphlab::dataframe_t>" (const variant_type& v)
    const unity_sgraph_base_ptr& variant_get_graph "graphlab::variant_get_ref<std::shared_ptr<graphlab::unity_sgraph_base>>"  (const variant_type& v)
    const model_base_ptr& variant_get_model "graphlab::variant_get_ref<std::shared_ptr<graphlab::model_base>>" (const variant_type& v)
    const unity_sframe_base_ptr& variant_get_sframe "graphlab::variant_get_ref<std::shared_ptr<graphlab::unity_sframe_base>>" (const variant_type& v)
    const unity_sarray_base_ptr& variant_get_sarray "graphlab::variant_get_ref<std::shared_ptr<graphlab::unity_sarray_base>>" (const variant_type& v)
    const map[string, variant_type]& variant_get_variant_map "graphlab::variant_get_ref<graphlab::variant_map_type>" (const variant_type& v)
    const vector[variant_type]& variant_get_variant_vector "graphlab::variant_get_ref<graphlab::variant_vector_type>" (const variant_type& v)


    void variant_set_flexible_type "graphlab::variant_set_value<graphlab::flexible_type>" (variant_type& v, flexible_type& f)
    void variant_set_dataframe "graphlab::variant_set_value<graphlab::dataframe_t>" (variant_type& v, gl_dataframe& f)
    void variant_set_graph "graphlab::variant_set_value<std::shared_ptr<graphlab::unity_sgraph_base>>" (variant_type& v, unity_sgraph_base_ptr f)
    void variant_set_model "graphlab::variant_set_value<std::shared_ptr<graphlab::model_base>>" (variant_type& v, model_base_ptr& f)
    void variant_set_sframe "graphlab::variant_set_value<std::shared_ptr<graphlab::unity_sframe_base>>" (variant_type& v, unity_sframe_base_ptr& f)
    void variant_set_sarray "graphlab::variant_set_value<std::shared_ptr<graphlab::unity_sarray_base>>" (variant_type& v, unity_sarray_base_ptr& f)
    void variant_set_variant_map "graphlab::variant_set_value<graphlab::variant_map_type>" (variant_type& v, const map[string, variant_type]& f)
    void variant_set_variant_vector "graphlab::variant_set_value<graphlab::variant_vector_type>" (variant_type& v, const vector[variant_type]& f)


ctypedef map[string, variant_type] variant_map_type
ctypedef map[string, variant_type].iterator variant_map_type_iterator
ctypedef vector[variant_type] variant_vector_type
ctypedef vector[variant_type].iterator variant_vector_type_iterator

cdef dict to_dict(PyCommClient cli, variant_map_type& d)
cdef list to_vector(PyCommClient cli, variant_vector_type& d)

cdef variant_map_type from_dict(dict d) except *
cdef variant_vector_type from_list(list d) except *


cdef variant_type from_value(object d) except *
cdef to_value(PyCommClient cli, variant_type& v)

cdef extern from "<memory>" namespace 'std':
    shared_ptr[variant_type] make_shared_variant "std::make_shared<graphlab::variant_type>"(const variant_type&)
