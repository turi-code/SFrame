# cython: c_string_type=bytes, c_string_encoding=utf8
'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''
#cython libcpp types
from libcpp.vector cimport vector
from libcpp.string cimport string
from libcpp.map cimport map
from libcpp.set cimport set as stdset
from libcpp.pair cimport pair

#### Define flexible type base types ####
# From "<flexible_type/flexible_type_base_types.hpp>" namespace "graphlab":
ctypedef long long flex_int
ctypedef double flex_float
ctypedef pair[long long,int] pflex_date_time
ctypedef vector[double] flex_vec
ctypedef string flex_string
ctypedef vector[flexible_type] flex_list
ctypedef vector[pair[flexible_type, flexible_type]] flex_dict


####Flex_image externs
cdef extern from "<image/image_type.hpp>" namespace "graphlab":
   cdef cppclass flex_image:
        flex_image()
        flex_image(char* image_data, size_t height, size_t width, size_t channels, \
                   size_t image_data_size, int version, int format)
        const char* get_image_data()
        size_t m_height
        size_t m_width
        size_t m_channels
        size_t m_image_data_size
        char m_format
        char m_version

#### Externs from flexible_type.hpp
cdef extern from "<flexible_type/flexible_type.hpp>" namespace "graphlab":

    cdef enum flex_type_enum:
        INTEGER         "graphlab::flex_type_enum::INTEGER"  = 0
        FLOAT           "graphlab::flex_type_enum::FLOAT"    = 1
        STRING          "graphlab::flex_type_enum::STRING"   = 2
        VECTOR          "graphlab::flex_type_enum::VECTOR"   = 3
        LIST            "graphlab::flex_type_enum::LIST"     = 4
        DICT            "graphlab::flex_type_enum::DICT"     = 5
        DATETIME        "graphlab::flex_type_enum::DATETIME" = 6
        UNDEFINED       "graphlab::flex_type_enum::UNDEFINED"= 7
        IMAGE           "graphlab::flex_type_enum::IMAGE"    = 8

    cdef cppclass flexible_type:
        flexible_type()
        flexible_type(flex_type_enum)
        flexible_type& soft_assign(const flexible_type& other) except +

        flex_type_enum get_type()
        # Cython bug:
        # we should put "except + TypeError" after the delcaration,
        # but it does not compile.
        const flex_int& get_int "get<graphlab::flex_int>"()
        pflex_date_time get_date_time "get_date_time_as_timestamp_and_offset"()
        int get_microsecond "get_date_time_microsecond"()
        const flex_float& get_double "get<graphlab::flex_float>"()
        const flex_string& get_string "get<graphlab::flex_string>"()
        const flex_vec& get_vec "get<graphlab::flex_vec>"()
        flex_vec& get_vec_m "mutable_get<graphlab::flex_vec>"()
        const flex_list& get_list "get<graphlab::flex_list>"()
        flex_list& get_list_m "mutable_get<graphlab::flex_list>"()
        const flex_dict& get_dict "get<graphlab::flex_dict>"()
        flex_dict& get_dict_m "mutable_get<graphlab::flex_dict>"()
        const flex_image& get_img "get<graphlab::flex_image>"()

        flex_float as_double "to<graphlab::flex_float>"()
        flex_list as_list "to<graphlab::flex_list>"()

        flexible_type& set_int "operator=<int64_t>"(const flex_int& other)
        flexible_type& set_date_time "set_date_time_from_timestamp_and_offset"(const pflex_date_time& other, int)
        flexible_type& set_double "operator=<double>"(const flex_float& other)
        flexible_type& set_string "operator=<std::string>"(const flex_string& other)
        flexible_type& set_vec "operator=<std::vector<double> >"(const flex_vec& other)
        flexible_type& set_list "operator=<graphlab::flex_list>"(const flex_list& other)
        flexible_type& set_dict "operator=<graphlab::flex_dict>"(const flex_dict& other)
        flexible_type& set_img "operator=<graphlab::flex_image>"(const flex_image& other)
        void reset()

    cdef flexible_type FLEX_UNDEFINED
    cdef void swap "std::swap"(flexible_type&, flexible_type&)

    cdef int TIMEZONE_RESOLUTION_IN_SECONDS "graphlab::flex_date_time::TIMEZONE_RESOLUTION_IN_SECONDS"
    cdef float TIMEZONE_RESOLUTION_IN_HOURS "graphlab::flex_date_time::TIMEZONE_RESOLUTION_IN_HOURS"
    cdef int EMPTY_TIMEZONE "graphlab::flex_date_time::EMPTY_TIMEZONE"
    cdef string flex_type_enum_to_name(flex_type_enum)
    cdef bint flex_type_is_convertible(flex_type_enum, flex_type_enum)

### Other unity types ###
ctypedef map[string, flexible_type] gl_options_map

# If we just want to work with the enum types, use these.
cdef flex_type_enum flex_type_enum_from_pytype(type t) except *
cdef type pytype_from_flex_type_enum(flex_type_enum e)
cpdef type pytype_from_dtype(object dt)
cdef flex_type_enum flex_type_from_dtype(object dt)
cpdef type pytype_from_array_typecode(str a)

cpdef type infer_type_of_list(list l)
cpdef type infer_type_of_sequence(object t)

#/**************************************************************************/
#/*                                                                        */
#/*            Converting from python objects to flexible_type             */
#/*                                                                        */
#/**************************************************************************/
cdef flexible_type flexible_type_from_pyobject(object) except *
cdef gl_options_map gl_options_map_from_pydict(dict) except *
cdef flex_list common_typed_flex_list_from_iterable(object, flex_type_enum* common_type) except *
cdef flex_list flex_list_from_iterable(object) except *
cdef flex_list flex_list_from_typed_iterable(object, flex_type_enum t, bint ignore_cast_failure) except *
cdef process_common_typed_list(flexible_type* out_ptr, list v, flex_type_enum common_type)

#/**************************************************************************/
#/*                                                                        */
#/*            Converting from flexible_type to python objects             */
#/*                                                                        */
#/**************************************************************************/
cdef object pyobject_from_flexible_type(const flexible_type&)
cdef list   pylist_from_flex_list(const flex_list&)
cdef dict   pydict_from_gl_options_map(const gl_options_map&)

# In some cases, a numeric list can be converted to a vector.  This
# function checks for that case, and converts the underlying type as
# needed.
cdef check_list_to_vector_translation(flexible_type& v)
