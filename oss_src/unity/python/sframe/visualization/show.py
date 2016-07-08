'''
Copyright (C) 2016 Turi
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''
from multipledispatch import dispatch as _dispatch

show_dispatch = _dispatch

@show_dispatch(object)
def show(obj, **kwargs):
    raise NotImplementedError("Show for object type " + str(type(obj)))
