'''
Created on Nov 6, 2011

@author: sean
'''
import unittest
from ...decompiler.tests import Base

class ListComprehension(Base):
    
    def test_comp1(self):
        stmnt = '[a for b in c]'
        self.statement(stmnt)

    def test_comp2(self):
        stmnt = '[a() +1 for b in c]'
        self.statement(stmnt)

    def test_comp3(self):
        stmnt = 'y = [a() +1 for b in c]'
        self.statement(stmnt)

    def test_comp_ifs(self):
        stmnt = 'y = [a() +1 for b in c if asdf]'
        self.statement(stmnt)

    def test_comp_ifs1(self):
        stmnt = 'y = [a() +1 for b in c if asdf if asd]'
        self.statement(stmnt)

    def test_comp_ifs2(self):
        stmnt = 'y = [a() +1 for b in c if asdf if not asd]'
        self.statement(stmnt)
    
    @unittest.expectedFailure
    def test_multi_comp1(self):
        stmnt = '[a for b in c for d in e]'
        self.statement(stmnt)

    @unittest.expectedFailure
    def test_multi_comp2(self):
        stmnt = '[a() +1 for b in c for d in e]'
        self.statement(stmnt)

    @unittest.expectedFailure
    def test_multi_comp3(self):
        stmnt = 'y = [a() +1 for b in c for d in e]'
        self.statement(stmnt)

    @unittest.expectedFailure
    def test_multi_comp_ifs(self):
        stmnt = 'y = [a() +1 for b in c if asdf for d in e]'
        self.statement(stmnt)

    @unittest.expectedFailure
    def test_multi_comp_ifs1(self):
        stmnt = 'y = [a() +1 for b in c if asdf if asd for d in e if this]'
        self.statement(stmnt)

    @unittest.expectedFailure
    def test_multi_comp_ifs2(self):
        stmnt = 'y = [a() +1 for b in c for d in e if adsf]'
        self.statement(stmnt)
    

class SetComprehension(Base):
    
    def test_comp1(self):
        stmnt = '{a for b in c}'
        self.statement(stmnt)

    def test_comp2(self):
        stmnt = '{a() +1 for b in c}'
        self.statement(stmnt)

    def test_comp3(self):
        stmnt = 'y = {a() +1 for b in c}'
        self.statement(stmnt)

    def test_comp_ifs(self):
        stmnt = 'y = {a() +1 for b in c if asdf}'
        self.statement(stmnt)

    def test_comp_ifs1(self):
        stmnt = 'y = {a() +1 for b in c if asdf if asd}'
        self.statement(stmnt)

    def test_comp_ifs2(self):
        stmnt = 'y = {a() +1 for b in c if asdf if not asd}'
        self.statement(stmnt)
    
    @unittest.expectedFailure
    def test_multi_comp1(self):
        stmnt = '{a for b in c for d in e}'
        self.statement(stmnt)

    @unittest.expectedFailure
    def test_multi_comp2(self):
        stmnt = '{a() +1 for b in c for d in e}'
        self.statement(stmnt)

    @unittest.expectedFailure
    def test_multi_comp3(self):
        stmnt = 'y = {a() +1 for b in c for d in e}'
        self.statement(stmnt)

    @unittest.expectedFailure
    def test_multi_comp_ifs(self):
        stmnt = 'y = {a() +1 for b in c if asdf for d in e}'
        self.statement(stmnt)

    @unittest.expectedFailure
    def test_multi_comp_ifs1(self):
        stmnt = 'y = {a() +1 for b in c if asdf if asd for d in e if this}'
        self.statement(stmnt)

    @unittest.expectedFailure
    def test_multi_comp_ifs2(self):
        stmnt = 'y = {a() +1 for b in c for d in e if adsf}'
        self.statement(stmnt)
    

class DictComprehension(Base):
    
    def test_comp1(self):
        stmnt = '{a:q for b in c}'
        self.statement(stmnt)

    def test_comp2(self):
        stmnt = '{a() +1:q for b in c}'
        self.statement(stmnt)

    def test_comp3(self):
        stmnt = 'y = {a() +1:q for b in c}'
        self.statement(stmnt)

    def test_comp_ifs(self):
        stmnt = 'y = {a() +1:q for b in c if asdf}'
        self.statement(stmnt)

    def test_comp_ifs1(self):
        stmnt = 'y = {a() +1:q for b in c if asdf if asd}'
        self.statement(stmnt)

    def test_comp_ifs2(self):
        stmnt = 'y = {a() +1:q for b in c if asdf if not asd}'
        self.statement(stmnt)
    
    @unittest.expectedFailure
    def test_multi_comp1(self):
        stmnt = '{a:q for b in c for d in e}'
        self.statement(stmnt)

    @unittest.expectedFailure
    def test_multi_comp2(self):
        stmnt = '{a():q +1 for b in c for d in e}'
        self.statement(stmnt)

    @unittest.expectedFailure
    def test_multi_comp3(self):
        stmnt = 'y = {a() +1:q for b in c for d in e}'
        self.statement(stmnt)

    @unittest.expectedFailure
    def test_multi_comp_ifs(self):
        stmnt = 'y = {a() +1:q for b in c if asdf for d in e}'
        self.statement(stmnt)

    @unittest.expectedFailure
    def test_multi_comp_ifs1(self):
        stmnt = 'y = {a() +1:q for b in c if asdf if asd for d in e if this}'
        self.statement(stmnt)

    @unittest.expectedFailure
    def test_multi_comp_ifs2(self):
        stmnt = 'y = {a() +1:q for b in c for d in e if adsf}'
        self.statement(stmnt)
    

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
    
