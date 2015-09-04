'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''
cdef int access_null_ptr():
  cdef int* p = NULL
  p[0] = 10
  return p[0]

def bad_memory_access_fun():
  access_null_ptr()

def force_exit_fun():
  exit(0)
