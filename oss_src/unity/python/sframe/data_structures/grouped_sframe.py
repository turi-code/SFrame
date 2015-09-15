"""
An interface for accessing an SFrame that is grouped by the values it contains
in one or more columns.
"""

'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the DATO-PYTHON-LICENSE file for details.
'''
class GroupedSFrame(object):
    """
    Left undocumented intentionally.
    """
    def __init__(self, sframe, key_columns):
        from .. import extensions
        self._sf_group = extensions.grouped_sframe()
        if isinstance(key_columns, str):
            key_columns = [key_columns]

        if not isinstance(key_columns, list):
            raise TypeError("Must give key columns as str or list.")
        self._sf_group.group(sframe, key_columns, False)

    def get_group(self, name):
        if not isinstance(name, list):
            name = [name]

        name.append(None)
        return self._sf_group.get_group(name)

    def groups(self):
        return self._sf_group.groups()

    def num_groups(self):
        return self._sf_group.num_groups()

    def __iter__(self):
        def generator():
            elems_at_a_time = 16
            self._sf_group.begin_iterator()
            ret = self._sf_group.iterator_get_next(elems_at_a_time)
            while(True):
                for j in ret:
                    yield tuple(j)

                if len(ret) == elems_at_a_time:
                    ret = self._sf_group.iterator_get_next(elems_at_a_time)
                else:
                    break

        return generator()
