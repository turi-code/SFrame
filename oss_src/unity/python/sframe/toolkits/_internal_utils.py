'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''
##\internal
"""@package graphlab.toolkits

This module defines the (internal) utility functions used by the toolkits.
"""

from ..data_structures.sframe import SArray as _SArray
from ..data_structures.sframe import SFrame as _SFrame
from ..data_structures.sgraph import SGraph as _SGraph
from ..data_structures.sgraph import Vertex as _Vertex
from ..data_structures.sgraph import Edge as _Edge
from ..cython.cy_sarray import UnitySArrayProxy
from ..cython.cy_sframe import UnitySFrameProxy
from ..cython.cy_graph import UnityGraphProxy
from ..toolkits._main import ToolkitError

import json
import logging as _logging


_proxy_map = {UnitySFrameProxy: (lambda x: _SFrame(_proxy=x)),
              UnitySArrayProxy: (lambda x: _SArray(_proxy=x)),
              UnityGraphProxy: (lambda x: _SGraph(_proxy=x))}


def _add_docstring(format_dict):
  """
  Format a doc-string on the fly.
  @arg format_dict: A dictionary to format the doc-strings
  Example:

    @add_docstring({'context': __doc_string_context})
    def predict(x):
      '''
      {context}
        >> model.predict(data)
      '''
      return x
  """
  def add_docstring_context(func):
      def wrapper(*args, **kwargs):
          return func(*args, **kwargs)
      wrapper.__doc__ = func.__doc__.format(**format_dict)
      return wrapper
  return add_docstring_context


def _SGraphFromJsonTree(json_str):
    """
    Convert the Json Tree to SGraph
    """
    g = json.loads(json_str)
    vertices = [_Vertex(x['id'],
                dict([(str(k), v) for k, v in x.iteritems() if k != 'id']))
                                                      for x in g['vertices']]
    edges = [_Edge(x['src'], x['dst'],
             dict([(str(k), v) for k, v in x.iteritems() if k != 'src' and k != 'dst']))
                                                      for x in g['edges']]
    sg = _SGraph().add_vertices(vertices)
    if len(edges) > 0:
        sg = sg.add_edges(edges)
    return sg

class _precomputed_field(object):
    def __init__(self, field):
        self.field = field

def _summarize_coefficients(top_coefs, bottom_coefs):
    """
    Return a tuple of sections and section titles.
    Sections are pretty print of model coefficients

    Parameters
    ----------
    top_coefs : SFrame of top k coefficients

    bottom_coefs : SFrame of bottom k coefficients

    Returns
    -------
    (sections, section_titles) : tuple
            sections : list
                summary sections for top/bottom k coefficients
            section_titles : list
                summary section titles
    """

    def get_row_name(row):
        if row['index'] == None:
            return row['name']
        else:
            return "%s[%s]" % (row['name'], row['index'])

    if len(top_coefs) == 0:
        top_coefs_list = [('No Positive Coefficients', _precomputed_field('') )]
    else:
        top_coefs_list = [ (get_row_name(row),
                            _precomputed_field(row['value'])) \
                            for row in top_coefs ]

    if len(bottom_coefs) == 0:
        bottom_coefs_list = [('No Negative Coefficients', _precomputed_field(''))]
    else:
        bottom_coefs_list = [ (get_row_name(row),
                            _precomputed_field(row['value'])) \
                            for row in bottom_coefs ]

    return ([top_coefs_list, bottom_coefs_list], \
                    [ 'Highest Positive Coefficients', 'Lowest Negative Coefficients'] )

def _toolkit_get_topk_bottomk(values, k=5):
    """
    Returns a tuple of the top k values from the positive and
    negative values in a SArray

    Parameters
    ----------
    values : SFrame of model coefficients

    k: Maximum number of largest postive and k lowest negative numbers to return

    Returns
    -------
    (topk_positive, bottomk_positive) : tuple
            topk_positive : list
                floats that represent the top 'k' ( or less ) positive
                values
            bottomk_positive : list
                floats that represent the top 'k' ( or less ) negative
                values
    """

    top_values = values.topk('value', k=k)
    top_values = top_values[top_values['value'] > 0]

    bottom_values = values.topk('value', k=k, reverse=True)
    bottom_values = bottom_values[bottom_values['value'] < 0]

    return (top_values, bottom_values)

def _toolkit_summary_dict_to_json(summary_dict):
    return json.dumps(summary_dict, allow_nan=False, ensure_ascii=False)

def _toolkit_summary_to_json(model, sections, section_titles):
    """
    Serialize model summary to JSON string.  JSON is an object with ordered arrays of
    section_titles and sections

    Parameters
    ----------
    model : Model object
    sections : Ordered list of lists (sections) of tuples (field,value)
      [
        [(field1, value1), (field2, value2)],
        [(field3, value3), (field4, value4)],

      ]
    section_titles : Ordered list of section titles

    """
    return _toolkit_summary_dict_to_json( \
                    _toolkit_serialize_summary_struct(  \
                        model, sections, section_titles) )

def __extract_model_summary_value(model, value):
    """
    Extract a model summary field value
    """
    field_value = None
    if isinstance(value, _precomputed_field):
        field_value = value.field
    else:
        field_value = model.get(value)
    if isinstance(field_value, float):
        try:
            field_value = round(field_value, 4)
        except:
            pass
    return field_value

def _toolkit_serialize_summary_struct(model, sections, section_titles):
    """
      Serialize model summary into a dict with ordered lists of sections and section titles

    Parameters
    ----------
    model : Model object
    sections : Ordered list of lists (sections) of tuples (field,value)
      [
        [(field1, value1), (field2, value2)],
        [(field3, value3), (field4, value4)],

      ]
    section_titles : Ordered list of section titles


    Returns
    -------
    output_dict : A dict with two entries:
                    'sections' : ordered list with tuples of the form ('label',value)
                    'section_titles' : ordered list of section labels
    """
    output_dict = dict()
    output_dict['sections'] = [ [ ( field[0], __extract_model_summary_value(model, field[1]) ) \
                                                                            for field in section ]
                                                                            for section in sections ]
    output_dict['section_titles'] = section_titles
    return output_dict

def _toolkit_repr_print(model, fields, section_titles, width=20):
    """
    Display a toolkit repr according to some simple rules.

    Parameters
    ----------
    model : GraphLab Create model

    fields: List of lists of tuples
        Each tuple should be (display_name, field_name), where field_name can
        be a string or a _precomputed_field object.


    section_titles: List of section titles, one per list in the fields arg.

    Example
    -------

        model_fields = [
            ("L1 penalty", 'l1_penalty'),
            ("L2 penalty", 'l2_penalty'),
            ("Examples", 'num_examples'),
            ("Features", 'num_features'),
            ("Coefficients", 'num_coefficients')]

        solver_fields = [
            ("Solver", 'solver'),
            ("Solver iterations", 'training_iterations'),
            ("Solver status", 'training_solver_status'),
            ("Training time (sec)", 'training_time')]

        training_fields = [
            ("Log-likelihood", 'training_loss')]

        fields = [model_fields, solver_fields, training_fields]:

        section_titles = ['Model description',
                          'Solver description',
                          'Training information']

        _toolkit_repr_print(model, fields, section_titles)
    """
    key_str = "{:<{}}: {}"

    ret = []
    ret.append(key_str.format("Class", width, model.__class__.__name__) + '\n')
    assert len(section_titles) == len(fields), \
        "The number of section titles ({0}) ".format(len(section_titles)) +\
        "doesn't match the number of groups of fields, {0}.".format(len(fields))

    summary_dict = _toolkit_serialize_summary_struct(model, fields, section_titles)

    for index, section_title in enumerate(summary_dict['section_titles']):
        ret.append(section_title)
        bar = '-' * len(section_title)
        ret.append(bar)
        section = summary_dict['sections'][index]
        for (field_title, field_value) in section:
            ret.append(key_str.format(field_title, width, field_value))
        ret.append("")
    return '\n'.join(ret)

def _map_unity_proxy_to_object(value):
    """
    Map returning value, if it is unity SFrame, SArray, map it
    """
    vtype = type(value)
    if vtype in _proxy_map:
        return _proxy_map[vtype](value)
    elif vtype == list:
        return [_map_unity_proxy_to_object(v) for v in value]
    elif vtype == dict:
        return {k:_map_unity_proxy_to_object(v) for k,v in value.items()}
    else:
        return value

def _toolkits_select_columns(dataset, columns):
    """
    Same as select columns but redirect runtime error to ToolkitError.
    """
    try:
        return dataset.select_columns(columns)

    except RuntimeError:
        missing_features = list(set(columns).difference(set(dataset.column_names())))
        raise ToolkitError("The following columns were expected but are " +
                           "missing: {}".format(missing_features))

def _raise_error_if_column_exists(dataset, column_name = 'dataset',
                            dataset_variable_name = 'dataset',
                            column_name_error_message_name = 'column_name'):
    """
    Check if a column exists in an SFrame with error message.
    """
    err_msg = 'The SFrame {0} must contain the column {1}.'.format(
                                                dataset_variable_name,
                                             column_name_error_message_name)
    if column_name not in dataset.column_names():
      raise ToolkitError(str(err_msg))

def _check_categorical_option_type(option_name, option_value, possible_values):
    """
    Check whether or not the requested option is one of the allowed values.
    """
    err_msg = '{0} is not a valid option for {0}. '.format(option_value, option_name)
    err_msg += ' Expected one of: '.format(possible_values)

    err_msg += ', '.join(map(str, possible_values))
    if option_value not in possible_values:
        raise ToolkitError(err_msg)

def _raise_error_if_not_sarray(dataset, variable_name="SFrame"):
    """
    Check if the input is an SArray. Provide a proper error
    message otherwise.
    """
    err_msg = "Input %s is not an SArray."
    if not isinstance(dataset, _SArray):
      raise ToolkitError(err_msg % variable_name)

def _raise_error_if_not_sframe(dataset, variable_name="SFrame"):
    """
    Check if the input is an SFrame. Provide a proper error
    message otherwise.
    """
    err_msg = "Input %s is not an SFrame. If it is a Pandas DataFrame,"
    err_msg += " you may use the to_sframe() function to convert it to an SFrame."

    if not isinstance(dataset, _SFrame):
      raise ToolkitError(err_msg % variable_name)

def _raise_error_if_sframe_empty(dataset, variable_name="SFrame"):
    """
    Check if the input is empty.
    """
    err_msg = "Input %s either has no rows or no columns. A non-empty SFrame "
    err_msg += "is required."

    if dataset.num_rows() == 0 or dataset.num_cols() == 0:
        raise ToolkitError(err_msg % variable_name)

def _raise_error_evaluation_metric_is_valid(metric, allowed_metrics):
    """
    Check if the input is an SFrame. Provide a proper error
    message otherwise.
    """

    err_msg = "Evaluation metric '%s' not recognized. The supported evaluation"
    err_msg += " metrics are (%s)."

    if metric not in allowed_metrics:
      raise ToolkitError(err_msg % (metric,
                          ', '.join(map(lambda x: "'%s'" % x, allowed_metrics))))

def _select_valid_features(dataset, features, valid_feature_types,
                           target_column=None):
    """
    Utility function for selecting columns of only valid feature types.

    Parameters
    ----------
    dataset: SFrame
        The input SFrame containing columns of potential features.

    features: list[str]
        List of feature column names.  If None, the candidate feature set is
        taken to be all the columns in the dataset.

    valid_feature_types: list[type]
        List of Python types that represent valid features.  If type is array.array,
        then an extra check is done to ensure that the individual elements of the array
        are of numeric type.  If type is dict, then an extra check is done to ensure
        that dictionary values are numeric.

    target_column: str
        Name of the target column.  If not None, the target column is excluded
        from the list of valid feature columns.

    Returns
    -------
    out: list[str]
        List of valid feature column names.  Warnings are given for each candidate
        feature column that is excluded.

    Examples
    --------
    # Select all the columns of type `str` in sf, excluding the target column named
    # 'rating'
    >>> valid_columns = _select_valid_features(sf, None, [str], target_column='rating')

    # Select the subset of columns 'X1', 'X2', 'X3' that has dictionary type or defines
    # numeric array type
    >>> valid_columns = _select_valid_features(sf, ['X1', 'X2', 'X3'], [dict, array.array])
    """
    if features is not None:
        if not hasattr(features, '__iter__'):
            raise TypeError("Input 'features' must be an iterable type.")

        if not all([isinstance(x, str) for x in features]):
            raise TypeError("Input 'features' must contain only strings.")

    ## Extract the features and labels
    if features is None:
        features = dataset.column_names()

    col_type_map = {
        col_name: col_type for (col_name, col_type) in
        zip(dataset.column_names(), dataset.column_types()) }

    valid_features = []
    for col_name in features:

        if col_name not in dataset.column_names():
            _logging.warning("Column '{}' is not in the input dataset.".format(col_name))

        elif col_name == target_column:
            _logging.warning("Excluding target column " + target_column +\
                             " as a feature.")

        elif col_type_map[col_name] not in valid_feature_types:
            _logging.warning("Column '{}' is excluded as a ".format(col_name) +
                             "feature due to invalid column type.")

        else:
            valid_features.append(col_name)

    if len(valid_features) == 0:
        raise ValueError("The dataset does not contain any valid feature columns. " +
            "Accepted feature types are " + str(valid_feature_types) + ".")

    return valid_features


def _numeric_param_check_range(variable_name, variable_value, range_bottom, range_top):
    """
    Checks if numeric parameter is within given range
    """
    err_msg = "%s must be between %i and %i"

    if variable_value < range_bottom or variable_value > range_top:
        raise ToolkitError(err_msg % (variable_name, range_bottom, range_top))

def _validate_row_label(dataset, label=None, default_label='__id'):
    """
    Validate a row label column. If the row label is not specified, a column is
    created with row numbers, named with the string in the `default_label`
    parameter.

    Parameters
    ----------
    dataset : SFrame
        Input dataset.

    label : str, optional
        Name of the column containing row labels.

    default_label : str, optional
        The default column name if `label` is not specified. A column with row
        numbers is added to the output SFrame in this case.

    Returns
    -------
    dataset : SFrame
        The input dataset, but with an additional row label column, *if* there
        was no input label.

    label : str
        The final label column name.
    """
    ## If no label is provided, set it to be a default and add a row number to
    #  dataset. Check that this new name does not conflict with an existing
    #  name.
    if not label:

        ## Try a bunch of variations of the default label to find one that's not
        #  already a column name.
        label_name_base = default_label
        label = default_label
        i = 1

        while label in dataset.column_names():
            label = label_name_base + '.{}'.format(i)
            i += 1

        dataset = dataset.add_row_number(column_name=label)

    ## Validate the label name and types.
    if not isinstance(label, str):
        raise TypeError("The row label column name '{}' must be a string.".format(label))

    if not label in dataset.column_names():
        raise ToolkitError("Row label column '{}' not found in the dataset.".format(label))

    if not dataset[label].dtype() in (str, int):
        raise TypeError("Row labels must be integers or strings.")

    ## Return the modified dataset and label
    return dataset, label



