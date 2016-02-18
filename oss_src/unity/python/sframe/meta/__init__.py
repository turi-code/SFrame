
from .decompiler.instructions import make_module
from .asttools.visitors.pysourcegen import dump_python_source
import sys
def decompile(code, mode='exec'):
    '''
    Decompile a code object into python ast.
    
    :param mode: must be 'exec' to compile a module or 'eval' to compile an expression.

    '''
    if mode == 'exec':
        return make_module(code)
    else:
        raise Exception("can not handle mode %r yet" % mode)
        
def test(stream=sys.stdout, descriptions=True, verbosity=2, failfast=False, buffer=False):
    '''
    Load and run the meta test suite.
    '''
    import unittest as _unit
    import os as _os
    star_dir = _os.path.dirname(__file__)
    test_suite = _unit.defaultTestLoader.discover(star_dir)
    runner = _unit.TextTestRunner(stream, descriptions, verbosity, failfast, buffer)
    runner.run(test_suite)
