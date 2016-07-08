"""
Dummy mocking module for sklearn.
When sklearn is not available we will import this module as graphlab.deps.sklearn,
and set HAS_SKLEARN to false. All methods that access sklearn should check the HAS_SKLEARN
flag, therefore, attributes/class/methods in this module should never be actually used.
"""

'''
Copyright (C) 2016 Turi
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''
