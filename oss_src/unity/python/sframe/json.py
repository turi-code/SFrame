'''
Copyright (C) 2016 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''

import array as _array
import datetime as _datetime

def _get_type_from_name(name):
    import sframe
    if name == 'int':
        return int
    if name == 'float':
        return float
    if name == 'str':
        return str
    if name == 'array':
        return _array.array
    if name == 'list':
        return list
    if name == 'dict':
        return dict
    if name == 'datetime':
        return _datetime.datetime
    if name == 'Image':
        return sframe.Image
    raise ValueError("Unsupported type hint provided. Expected one of: int, float, str, array, list, dict, datetime, or Image. Got %s." % name)

def _to_flex_type(obj):
    """
    Invertibly converts non-flexible_type object types to flexible_type.
    """
    import sframe
    if isinstance(obj, sframe.SArray):
        return _to_flex_type({
            "type": "SArray",
            "value": {
                "dtype": obj.dtype().__name__,
                "values": list(obj)
            }
        })
    if isinstance(obj, sframe.SFrame):
        return _to_flex_type({
            "type": "SFrame",
            "value": {
                name: obj[name] for name in obj.column_names()
            }
        })
    if isinstance(obj, dict):
        ret = {}
        for (k,v) in obj.iteritems():
            ret[_to_flex_type(k)] = _to_flex_type(v)
        return ret
    if isinstance(obj, list):
        return [_to_flex_type(v) for v in obj]
    return obj

def _from_flex_type(obj):
    """
    Invertibly converts flexible_type to some other types encoded as dict.
    """
    import sframe
    if isinstance(obj, dict):
        if len(obj.keys()) == 2 and \
            'type' in obj and \
            'value' in obj:
            if obj['type'] == 'SArray':
                return sframe.SArray(
                    obj['value']['values'],
                    dtype=_get_type_from_name(obj['value']['dtype'])
                )
            if obj['type'] == 'SFrame':
                columns = obj['value']
                return sframe.SFrame({
                    name: _from_flex_type(value) for (name,value) in columns.iteritems()
                })
    return obj

def dumps(serializable_object):
    """
    Dumps a serializable object to JSON. This API maps to the Python built-in
    json dumps method, with a few differences:

    * The return value is always valid JSON according to RFC 7159.
    * The input can be any of the following types:
        - SFrame
        - SArray
        - Image
        - int, long
        - float
        - datetime.datetime
        - recursive types of the above: list, dict, array.array
    """
    import sframe
    serializable_object = _to_flex_type(serializable_object)
    return sframe.extensions.json.dumps(serializable_object)

def loads(json_string):
    """
    Loads a serializable object from JSON. This API maps to the Python built-in
    json loads method, with a few differences:

    * The input string must be valid JSON according to RFC 7159.
    * The input must represent a serialized result produced by the `dumps`
      method in this module. If it does not the result will be undefined.
    """
    import sframe
    ret = sframe.extensions.json.loads(json_string)
    return _from_flex_type(ret)
