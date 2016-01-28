# cython: c_string_type=bytes, c_string_encoding=utf8
"""
Provides a translation layer for easing the use of callback
functions between cython and the c++ layer.
"""

# Provides exception 
cdef void register_exception(object exception)
