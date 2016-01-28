# cython: c_string_type=bytes, c_string_encoding=utf8
'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''
cdef extern from "stdlib.h":
  void abort()
