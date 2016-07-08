"""
Dummy mocking module for nltk.
When nltk is not available we will import this module as graphlab.deps.nltk,
and set HAS_NLTK to false. All methods that access nltk should check the
HAS_NLTK flag, therefore, attributes/class/methods in this module should never
be actually used.
"""

'''
Copyright (C) 2016 Turi
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''
