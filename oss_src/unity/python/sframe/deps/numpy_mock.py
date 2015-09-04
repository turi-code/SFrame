"""
Dummy mocking module for numpy.
When numpy is not avaialbe we will import this module as graphlab.deps.numpy,
and set HAS_NUMPY to false. All methods that access numpy should check the HAS_NUMPY
flag, therefore, attributes/class/methods in this module should never be actually used.
"""

'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''
