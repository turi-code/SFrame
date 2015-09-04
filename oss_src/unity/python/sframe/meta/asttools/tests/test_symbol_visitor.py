'''
Created on Aug 5, 2011

@author: sean
'''
from __future__ import print_function

import unittest
import ast
from ...asttools.visitors.symbol_visitor import get_symbols


class Test(unittest.TestCase):

    def assertHasSymbols(self, codestring, expected_symbols, ctxts=(ast.Load, ast.Store)):
        root = ast.parse(codestring)

        symbols = get_symbols(root, ctxts)

        self.assertEqual(symbols, expected_symbols)


    def test_simple(self):
        self.assertHasSymbols('a', {'a'})

    def test_load(self):
        self.assertHasSymbols('a', {'a'}, ast.Load)
        self.assertHasSymbols('a', set(), ast.Store)

    def test_store(self):
        self.assertHasSymbols('a = 1', {'a'}, ast.Store)
        self.assertHasSymbols('a = 1', set(), ast.Load)

    def test_store_item(self):
        self.assertHasSymbols('a[:] = 1', {'a'}, ast.Load)
        self.assertHasSymbols('a[:] = 1', set(), ast.Store)

    def test_store_attr(self):

        self.assertHasSymbols('a.b = 1', {'a'}, ast.Load)
        self.assertHasSymbols('a.b = 1', set(), ast.Store)

    def test_for(self):

        self.assertHasSymbols('for i in x:\n    a.b = 1', {'a', 'x'}, ast.Load)
        self.assertHasSymbols('for i in x:\n    a.b = 1', {'i'}, ast.Store)


if __name__ == '__main__':

    unittest.main()
