'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''
from .cy_flexible_type cimport flexible_type
from .cy_flexible_type cimport flex_type_enum, INTEGER, FLOAT, UNDEFINED
from .cy_flexible_type cimport common_typed_flex_list_from_iterable
from .cy_flexible_type cimport pylist_from_flex_list
from .cy_flexible_type cimport flex_type_from_dtype
from .cy_flexible_type cimport pytype_from_flex_type_enum
from math import isnan

HAS_PANDAS = False

try:
    import pandas as pd
    HAS_PANDAS = True
except:
    HAS_PANDAS = False


cdef bint is_pandas_dataframe(object v):
    if HAS_PANDAS:
        return isinstance(v, pd.core.frame.DataFrame)
    else:
        return False

cdef gl_dataframe gl_dataframe_from_pd(object df) except *:
    cdef gl_dataframe ret
    cdef flex_type_enum ftype = UNDEFINED
    cdef flex_list fl

    assert is_pandas_dataframe(df), 'Cannot convert object of type %s to gl_dataframe' % str(type(df))
    if len(set(df.columns.values)) != len(df.columns.values):
        raise ValueError('Input pandas dataframe column names must be unique')
    for colname in df.columns.values:

        ft_type = flex_type_from_dtype(df[colname].values.dtype)
        if ft_type == FLOAT or ft_type == INTEGER:
            fl = common_typed_flex_list_from_iterable(df[colname].values, &ftype)
        else:
            colcopy = [None if isinstance(x, float) and isnan(x) else x for x in df[colname].values]
            fl = common_typed_flex_list_from_iterable(colcopy, &ftype)


        # convert all NaNs to None
        ret.names.push_back(str(colname))
        ret.types[str(colname)] = ftype
        ret.values[str(colname)] = fl

    return ret


cdef gl_dataframe gl_dataframe_from_dict_of_arrays(dict df) except *:
    cdef gl_dataframe ret
    cdef flex_type_enum ftype
    cdef flex_list fl

    for key, value in sorted(df.iteritems()):
        ret.names.push_back(key)
        ret.values[str(key)] = common_typed_flex_list_from_iterable(value, &ftype)
        ret.types[str(key)] = ftype

    return ret

cdef pd_from_gl_dataframe(gl_dataframe& df):
    assert HAS_PANDAS, 'Cannot find pandas library'
    ret = pd.DataFrame()
    for _name in df.names:
        _type = pytype_from_flex_type_enum(df.types[_name])
        ret[_name] = pylist_from_flex_list(df.values[_name])
        if len(ret[_name]) == 0:
            """ special handling of empty list, we need to force the type information """
            ret[_name] = ret[_name].astype(_type)
    return ret 


### Test Util: convert dataframe to gl_dataframe and back ###
def _dataframe(object df):
    return pd_from_gl_dataframe(gl_dataframe_from_pd(df))
