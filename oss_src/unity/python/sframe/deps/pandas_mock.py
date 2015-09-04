""""
Dummy mocking module for pandas.
When pandas is not avaialbe we will import this module as graphlab.deps.pandas,
and set HAS_pandas to false. All methods that access pandas should check the HAS_pandas
flag, therefore, attributes/class/methods in this module should never be actually used.
"""

'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''
