'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''

from sys import version_info as _version_info

from ..deps import numpy as _np, HAS_NUMPY as _HAS_NUMPY

def __do_operation_recursively(operation_name):
    def inner(obj):
        if hasattr(obj, operation_name):
            try:
                operation = getattr(obj, operation_name)
                return operation()
            except:
                return obj
        elif isinstance(obj, dict):
            ret = {}
            for k,v in obj.items():
                ret[inner(k)] = inner(v)
            return ret
        elif isinstance(obj, list):
            return [inner(i) for i in obj]
        elif _HAS_NUMPY and isinstance(obj, _np.ndarray):
            return _np.array([inner(i) for i in obj])
        return obj

    return inner


if _version_info == 3:
    _encode = __do_operation_recursively('encode')
    _decode = __do_operation_recursively('decode')
else:
    _encode = lambda x : x
    _decode = lambda x : x
