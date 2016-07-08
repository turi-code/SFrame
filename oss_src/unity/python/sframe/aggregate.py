"""
Builtin aggregators for SFrame groupby operator.
"""

'''
Copyright (C) 2016 Turi
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''

from .util import _is_non_string_iterable

def SUM(src_column):
  """
  Builtin sum aggregator for groupby.

  Example: Get the sum of the rating column for each user. If
  src_column is of array type, if array's do not match in length a NoneType is
  returned in the destination column.

  >>> sf.groupby("user",
  ...            {'rating_sum':gl.aggregate.SUM('rating')})

  """
  return ("__builtin__sum__", [src_column])

def ARGMAX(agg_column,out_column):
  """
  Builtin arg maximum aggregator for groupby

  Example: Get the movie with maximum rating per user.

  >>> sf.groupby("user",
  ...            {'best_movie':gl.aggregate.ARGMAX('rating','movie')})
  """
  return ("__builtin__argmax__",[agg_column,out_column])

def ARGMIN(agg_column,out_column):
  """
  Builtin arg minimum aggregator for groupby

  Example: Get the movie with minimum rating per user.

  >>> sf.groupby("user",
  ...            {'best_movie':gl.aggregate.ARGMIN('rating','movie')})

  """
  return ("__builtin__argmin__",[agg_column,out_column])

def MAX(src_column):
  """
  Builtin maximum aggregator for groupby

  Example: Get the maximum rating of each user.

  >>> sf.groupby("user",
  ...            {'rating_max':gl.aggregate.MAX('rating')})

  """
  return ("__builtin__max__", [src_column])

def MIN(src_column):
  """
  Builtin minimum aggregator for groupby

  Example: Get the minimum rating of each user.

  >>> sf.groupby("user",
  ...            {'rating_min':gl.aggregate.MIN('rating')})

  """
  return ("__builtin__min__", [src_column])


def COUNT(*args):
  """
  Builtin count aggregator for groupby

  Example: Get the number of occurrences of each user.

  >>> sf.groupby("user",
  ...            {'count':gl.aggregate.COUNT()})

  """
  # arguments if any are ignored
  return ("__builtin__count__", [""])



def AVG(src_column):
  """
  Builtin average aggregator for groupby. Synonym for gl.aggregate.MEAN. If
  src_column is of array type, and if array's do not match in length a NoneType is
  returned in the destination column.

  Example: Get the average rating of each user.

  >>> sf.groupby("user",
  ...            {'rating_avg':gl.aggregate.AVG('rating')})

  """
  return ("__builtin__avg__", [src_column])


def MEAN(src_column):
  """
  Builtin average aggregator for groupby. Synonym for gl.aggregate.AVG. If
  src_column is of array type, and if array's do not match in length a NoneType is
  returned in the destination column.


  Example: Get the average rating of each user.

  >>> sf.groupby("user",
  ...            {'rating_mean':gl.aggregate.MEAN('rating')})

  """
  return ("__builtin__avg__", [src_column])

def VAR(src_column):
  """
  Builtin variance aggregator for groupby. Synonym for gl.aggregate.VARIANCE

  Example: Get the rating variance of each user.

  >>> sf.groupby("user",
  ...            {'rating_var':gl.aggregate.VAR('rating')})

  """
  return ("__builtin__var__", [src_column])


def VARIANCE(src_column):
  """
  Builtin variance aggregator for groupby. Synonym for gl.aggregate.VAR

  Example: Get the rating variance of each user.

  >>> sf.groupby("user",
  ...            {'rating_var':gl.aggregate.VARIANCE('rating')})

  """
  return ("__builtin__var__", [src_column])


def STD(src_column):
  """
  Builtin standard deviation aggregator for groupby. Synonym for gl.aggregate.STDV

  Example: Get the rating standard deviation of each user.

  >>> sf.groupby("user",
  ...            {'rating_std':gl.aggregate.STD('rating')})

  """
  return ("__builtin__stdv__", [src_column])


def STDV(src_column):
  """
  Builtin standard deviation aggregator for groupby. Synonym for gl.aggregate.STD

  Example: Get the rating standard deviation of each user.

  >>> sf.groupby("user",
  ...            {'rating_stdv':gl.aggregate.STDV('rating')})

  """
  return ("__builtin__stdv__", [src_column])


def SELECT_ONE(src_column):
  """
  Builtin aggregator for groupby which selects one row in the group.

  Example: Get one rating row from a user.

  >>> sf.groupby("user",
  ...            {'rating':gl.aggregate.SELECT_ONE('rating')})

  If multiple columns are selected, they are guaranteed to come from the
  same row. for instance:
  >>> sf.groupby("user",
  ...            {'rating':gl.aggregate.SELECT_ONE('rating')},
  ...            {'item':gl.aggregate.SELECT_ONE('item')})

  The selected 'rating' and 'item' value for each user will come from the
  same row in the SFrame.

  """
  return ("__builtin__select_one__", [src_column])

def CONCAT(src_column, dict_value_column = None):
  """
  Builtin aggregator that combines values from one or two columns in one group
  into either a dictionary value or list value.

  If only one column is given, then the values of this column are
  aggregated into a list.  Order is not preserved.  For example:

  >>> sf.groupby(["user"],
  ...     {"friends": gl.aggregate.CONCAT("friend")})

  would form a new column "friends" containing values in column
  "friend" aggregated into a list of friends.

  If `dict_value_column` is given, then the aggregation forms a dictionary with
  the keys taken from src_column and the values taken from `dict_value_column`.
  For example:

  >>> sf.groupby(["document"],
  ...     {"word_count": gl.aggregate.CONCAT("word", "count")})

  would aggregate words from column "word" and counts from column
  "count" into a dictionary with keys being words and values being
  counts.
  """
  if dict_value_column == None:
    return ("__builtin__concat__list__", [src_column])
  else:
    return ("__builtin__concat__dict__", [src_column, dict_value_column])


def QUANTILE(src_column, *args):
    """
    Builtin approximate quantile aggregator for groupby.
    Accepts as an argument, one or more of a list of quantiles to query.
    For instance:

    To extract the median

    >>> sf.groupby("user",
    ...   {'rating_quantiles':gl.aggregate.QUANTILE('rating', 0.5)})

    To extract a few quantiles

    >>> sf.groupby("user",
    ...   {'rating_quantiles':gl.aggregate.QUANTILE('rating', [0.25,0.5,0.75])})

    Or equivalently

    >>> sf.groupby("user",
    ...     {'rating_quantiles':gl.aggregate.QUANTILE('rating', 0.25,0.5,0.75)})

    The returned quantiles are guaranteed to have 0.5% accuracy. That is to say,
    if the requested quantile is 0.50, the resultant quantile value may be
    between 0.495 and 0.505 of the true quantile.
    """
    if len(args) == 1:
        quantiles = args[0]
    else:
        quantiles = list(args)

    if not _is_non_string_iterable(quantiles):
        quantiles = [quantiles]
    query = ",".join([str(i) for i in quantiles])
    return ("__builtin__quantile__[" + query + "]", [src_column])


def COUNT_DISTINCT(src_column):
  """
  Builtin unique counter for groupby. Counts the number of unique values

  Example: Get the number of unique ratings produced by each user.

  >>> sf.groupby("user",
  ...    {'rating_distinct_count':gl.aggregate.COUNT_DISTINCT('rating')})

  """
  return ("__builtin__count__distinct__", [src_column])


def DISTINCT(src_column):
  """
  Builtin distinct values for groupby. Returns a list of distinct values.

  >>> sf.groupby("user",
  ...       {'rating_distinct':gl.aggregate.DISTINCT('rating')})

  """
  return ("__builtin__distinct__", [src_column])

def FREQ_COUNT(src_column):
  """
  Builtin frequency counts for groupby. Returns a dictionary where the key is
  the `src_column` and the value is the number of times each value occurs.

  >>> sf.groupby("user",
  ...       {'rating_distinct':gl.aggregate.FREQ_COUNT('rating')})

  """
  return ("__builtin__freq_count__", [src_column])


