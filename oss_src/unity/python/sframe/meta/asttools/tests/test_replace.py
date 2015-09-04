'''
Created on Aug 5, 2011

@author: sean
'''
from __future__ import print_function

import unittest
from ...asttools.mutators.replace_mutator import replace_nodes
import ast
from ...asttools.tests import assert_ast_eq

class Test(unittest.TestCase):

    def test_replace_name(self):

        root = ast.parse('a = 1')

        name_a = root.body[0].targets[0]
        name_b = ast.Name(id='b', ctx=ast.Store())
        replace_nodes(root, name_a, name_b)

        expected = ast.parse('b = 1')
        assert_ast_eq(self, root, expected)


    def test_replace_non_existant(self):

        root = ast.parse('a = 1')

        name_a = root.body[0].targets[0]
        name_b = ast.Name(id='b', ctx=ast.Store())
        replace_nodes(root, name_b, name_a)

        expected = ast.parse('a = 1')
        assert_ast_eq(self, root, expected)

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
