'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''
import unittest
from ..data_structures.sarray import SArray
from ..data_structures.sframe import SFrame

import sys
if sys.version_info.major > 2:
    long = int

class VariantCheckTest(unittest.TestCase):

    def identical(self, reference, b):
        if type(reference) in [int, long]:
            self.assertIn(type(b), [int, long])
        else:
            self.assertEquals(type(reference), type(b))
        if isinstance(reference, list):
            self.assertEquals(len(reference), len(b))
            for i in range(len(reference)):
                self.identical(reference[i], b[i])
        if isinstance(reference, dict):
            self.assertEqual(sorted(reference.keys()), sorted(b.keys()))
            for i in reference:
                self.identical(reference[i], b[i])
        if isinstance(reference, SArray):
            self.identical(list(reference), list(b))
        if isinstance(reference, SFrame):
            self.identical(list(reference), list(b))

    def variant_turnaround(self, reference, expected_result=None):
        if expected_result is None:
            expected_result = reference
        from ..extensions import _demo_identity
        self.identical(expected_result, _demo_identity(reference))

    def test_variant_check(self):
        sa = SArray([1,2,3,4,5])
        sf = SFrame({'a':sa})
        import array
        self.variant_turnaround(1)
        self.variant_turnaround(1.0)
        self.variant_turnaround(array.array('d', [1.0, 2.0, 3.0]))
         # numeric lists currently converts to array
        self.variant_turnaround([1, 2, 3], array.array('d',[1,.0,2.0,3.0]))
        self.variant_turnaround("abc")
        self.variant_turnaround(["abc", "def"])
        self.variant_turnaround({'a':1,'b':'c'})
        self.variant_turnaround({'a':[1,2,'d'],'b':['a','b','c']})
         # numeric lists currently converts to array
        self.variant_turnaround({'a':[1,2,3],'b':['a','b','c']},
                                {'a':array.array('d',[1,2,3]),'b':['a','b','c']})
        self.variant_turnaround(sa)
        self.variant_turnaround(sf)
        self.variant_turnaround([sa,sf])
        self.variant_turnaround([sa,sa])
        self.variant_turnaround([sf,sf])
        self.variant_turnaround({'a':sa, 'b':sf, 'c':['a','b','c','d']})
        self.variant_turnaround({'a':[{'a':1, 'b':2}], 'b':[{'a':3}]})
        self.variant_turnaround({'a':[{'a':sa, 'b': sa}], 'b':[{'a':sa}]})
        self.variant_turnaround({'a': [sa, {'c':sa, 'd': sa}], 'e':[{'f':sa}]})
        self.variant_turnaround({'a':[sa,{'a':sa}]})
        self.variant_turnaround({'a':[{'a':sa,'b':'c'}]})
        self.variant_turnaround({'a':[sa,{'a':sa,'b':'c'}]})
        self.variant_turnaround({'a':[sa,sf,{'a':sa,'b':'c'}],
            'b':sf, 'c':['a','b','c','d']})

        
