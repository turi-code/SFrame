'''
Copyright (C) 2016 Turi
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''
from .sframe import SFrame
from ..cython.context import debug_trace as cython_context
from ..util import _is_non_string_iterable
from .sarray import SArray, _create_sequential_sarray
import copy

VERTEX_GFRAME = 0
EDGE_GFRAME = 1


class GFrame(SFrame):
    """
    GFrame is similar to SFrame but is associated with an SGraph.

    - GFrame can be obtained from either the `vertices` or `edges`
    attributed in any SGraph:

    >>> import graphlab
    >>> g = graphlab.load_sgraph(...)
    >>> vertices_gf = g.vertices
    >>> edges_gf = g.edges

    - GFrame has the same API as SFrame:

    >>> sa = vertices_gf['pagerank']

    >>> # column lambda transform
    >>> vertices_gf['pagerank'] = vertices_gf['pagerank'].apply(lambda x: 0.15 + 0.85 * x)

    >>> # frame lambda transform
    >>> vertices_gf['score'] = vertices_gf.apply(lambda x: 0.2 * x['triangle_count'] + 0.8 * x['pagerank'])

    >>> del vertices_gf['pagerank']

    - GFrame can be converted to SFrame:

    >>> # extract an SFrame
    >>> sf = vertices_gf.__to_sframe__()
    """
    def __init__(self, graph, gframe_type):
        self.__type__ = gframe_type
        self.__graph__ = graph
        self.__sframe_cache__ = None
        self.__is_dirty__ = False

    def __to_sframe__(self):
        return copy.copy(self._get_cache())

#/**************************************************************************/
#/*                                                                        */
#/*                               Modifiers                                */
#/*                                                                        */
#/**************************************************************************/
    def add_column(self, data, name=""):
        """
        Adds the specified column to this SFrame.  The number of elements in
        the data given must match every other column of the SFrame.

        Parameters
        ----------
        data : SArray
            The 'column' of data.

        name : string
            The name of the column. If no name is given, a default name is chosen.
        """
        # Check type for pandas dataframe or SArray?
        if not isinstance(data, SArray):
            raise TypeError("Must give column as SArray")
        if not isinstance(name, str):
            raise TypeError("Invalid column name: must be str")

        self.__is_dirty__ = True
        with cython_context():
            if self._is_vertex_frame():
                graph_proxy = self.__graph__.__proxy__.add_vertex_field(data.__proxy__, name)
                self.__graph__.__proxy__ = graph_proxy
            elif self._is_edge_frame():
                graph_proxy = self.__graph__.__proxy__.add_edge_field(data.__proxy__, name)
                self.__graph__.__proxy__ = graph_proxy

    def add_columns(self, datalist, namelist):
        """
        Adds columns to the SFrame.  The number of elements in all columns must
        match every other column of the SFrame.

        Parameters
        ----------
        datalist : list of SArray
            A list of columns

        namelist : list of string
            A list of column names. All names must be specified.
        """
        if not _is_non_string_iterable(datalist):
            raise TypeError("datalist must be an iterable")
        if not _is_non_string_iterable(namelist):
            raise TypeError("namelist must be an iterable")

        if not all([isinstance(x, SArray) for x in datalist]):
            raise TypeError("Must give column as SArray")
        if not all([isinstance(x, str) for x in namelist]):
            raise TypeError("Invalid column name in list: must all be str")
        for (data, name) in zip(datalist, namelist):
            self.add_column(data, name)

    def remove_column(self, name):
        """
        Removes the column with the given name from the SFrame.

        Parameters
        ----------
        name : string
            The name of the column to remove.
        """
        if name not in self.column_names():
            raise KeyError('Cannot find column %s' % name)
        self.__is_dirty__ = True
        try:
            with cython_context():
                if self._is_vertex_frame():
                    assert name != '__id', 'Cannot remove \"__id\" column'
                    graph_proxy = self.__graph__.__proxy__.delete_vertex_field(name)
                    self.__graph__.__proxy__ = graph_proxy
                elif self._is_edge_frame():
                    assert name != '__src_id', 'Cannot remove \"__src_id\" column'
                    assert name != '__dst_id', 'Cannot remove \"__dst_id\" column'
                    graph_proxy = self.__graph__.__proxy__.delete_edge_field(name)
                    self.__graph__.__proxy__ = graph_proxy
        except:
            self.__is_dirty__ = False
            raise

    def swap_columns(self, column_1, column_2):
        """
        Swaps the columns with the given names.

        Parameters
        ----------
        column_1 : string
            Name of column to swap

        column_2 : string
            Name of other column to swap
        """
        self.__is_dirty__ = True
        with cython_context():
            if self._is_vertex_frame():
                graph_proxy = self.__graph__.__proxy__.swap_vertex_fields(column_1, column_2)
                self.__graph__.__proxy__ = graph_proxy
            elif self._is_edge_frame():
                graph_proxy = self.__graph__.__proxy__.swap_edge_fields(column_1, column_2)
                self.__graph__.__proxy__ = graph_proxy

    def rename(self, names):
        """
        Rename the columns using the 'names' dict.  This changes the names of
        the columns given as the keys and replaces them with the names given as
        the values.

        Parameters
        ----------
        names : dict[string, string]
            Dictionary of [old_name, new_name]
        """
        if (type(names) is not dict):
            raise TypeError('names must be a dictionary: oldname -> newname')

        self.__is_dirty__ = True
        with cython_context():
            if self._is_vertex_frame():
                graph_proxy = self.__graph__.__proxy__.rename_vertex_fields(names.keys(), names.values())
                self.__graph__.__proxy__ = graph_proxy
            elif self._is_edge_frame():
                graph_proxy = self.__graph__.__proxy__.rename_edge_fields(names.keys(), names.values())
                self.__graph__.__proxy__ = graph_proxy

    def add_row_number(self, column_name='id', start=0):
        if type(column_name) is not str:
            raise TypeError("Must give column_name as str")

        if column_name in self.column_names():
            raise RuntimeError("Column name %s already exists" % str(column_name))

        if type(start) is not int:
            raise TypeError("Must give start as int")

        the_col = _create_sequential_sarray(self.num_rows(), start)

        self[column_name] = the_col

        return self


    def __setitem__(self, key, value):
        """
        A wrapper around add_column(s).  Key can be either a list or a str.  If
        value is an SArray, it is added to the SFrame as a column.  If it is a
        constant value (int, str, or float), then a column is created where
        every entry is equal to the constant value.  Existing columns can also
        be replaced using this wrapper.
        """
        if (key in ['__id', '__src_id', '__dst_id']):
            raise KeyError('Cannot modify column %s. Changing __id column will\
                    change the graph structure' % key)
        else:
            self.__is_dirty__ = True
            super(GFrame, self).__setitem__(key, value)

#/**************************************************************************/
#/*                                                                        */
#/*                               Read-only Accessor                       */
#/*                                                                        */
#/**************************************************************************/
    def num_rows(self):
        """
        Returns the number of rows.

        Returns
        -------
        out : int
            Number of rows in the SFrame.
        """
        if self._is_vertex_frame():
            return self.__graph__.summary()['num_vertices']
        elif self._is_edge_frame():
            return self.__graph__.summary()['num_edges']

    def num_cols(self):
        """
        Returns the number of columns.

        Returns
        -------
        out : int
            Number of columns in the SFrame.
        """
        return len(self.column_names())

    def column_names(self):
        """
        Returns the column names.

        Returns
        -------
        out : list[string]
            Column names of the SFrame.
        """
        if self._is_vertex_frame():
            return self.__graph__.__proxy__.get_vertex_fields()
        elif self._is_edge_frame():
            return self.__graph__.__proxy__.get_edge_fields()

    def column_types(self):
        """
        Returns the column types.

        Returns
        -------
        out : list[type]
            Column types of the SFrame.
        """
        if self.__type__ == VERTEX_GFRAME:
            return self.__graph__.__proxy__.get_vertex_field_types()
        elif self.__type__ == EDGE_GFRAME:
            return self.__graph__.__proxy__.get_edge_field_types()

#/**************************************************************************/
#/*                                                                        */
#/*                        Internal Private Methods                        */
#/*                                                                        */
#/**************************************************************************/
    def _get_cache(self):
        if self.__sframe_cache__ is None or self.__is_dirty__:
            if self._is_vertex_frame():
                self.__sframe_cache__ = self.__graph__.get_vertices()
            elif self._is_edge_frame():
                self.__sframe_cache__ = self.__graph__.get_edges()
            else:
                raise TypeError
        self.__is_dirty__ = False
        return self.__sframe_cache__

    def _is_vertex_frame(self):
        return self.__type__ == VERTEX_GFRAME

    def _is_edge_frame(self):
        return self.__type__ == EDGE_GFRAME

    @property
    def __proxy__(self):
        return self._get_cache().__proxy__
