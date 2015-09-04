##
## Copyright (C) 2015 Dato, Inc.
## All rights reserved.
##
## This software may be modified and distributed under the terms
## of the BSD license. See the LICENSE file for details.
##
## methods-sframe.r ---- R functions related to sframe

#' The number of columns of a sframe
#' @name ncol
#' @aliases ncol,sframe-method
#' @param x a sframe
#' @usage ncol(sf)
#' @export
#' @examples
#' sf <- sframe(id = 1:100, y = rnorm(100))
#' ncol(sf)
setMethod("ncol",
          signature = c("sframe"),
          function(x)
            return(x@backend$ncol()))


#' The number of rows of a sframe
#' @name nrow
#' @aliases nrow,sframe-method
#' @param x a \code{sframe}
#' @usage nrow(sf)
#' @export
#' @examples
#' sf <- sframe(id = 1:100, y = rnorm(100))
#' nrow(sf)
setMethod("nrow",
          signature = c("sframe"),
          function(x)
            return(x@backend$nrow()))

#' Column names of a sframe
#' @name colnames
#' @usage colnames(sf)
#' @aliases colnames,sframe-method colnames<-,sframe-method
#' @param x a \code{sframe}
#' @export colnames<-
#' @export colnames
#' @examples
#' sf <- sframe(id = 1:100, y = rnorm(100))
#' colnames(sf)
#' colnames(sf) <- c("a", "b")
setMethod("colnames",
          signature = c("sframe"),
          function(x, do.NULL = TRUE, prefix = "col")
            return(x@backend$colnames()))

setMethod("colnames<-",
          signature = c(x = "sframe"),
          function(x, value) {
            if (length(value) != ncol(x))
              stop("must specify colnames with the same length as ncol(x)")
            old_names <- x@backend$colnames()
            sf@backend$rename(old_names, value)
          })

setMethod("show",
          signature(object = "sframe"),
          function(object) {
            object@backend$show()
          })

#' First part of a sframe/sarray
#' @name head
#' @aliases head,sframe-method head,sarray-method
#' @param x a sframe
#' @param n number of rows to show. Default value is 10
#' @export
#' @usage head(sf)
#' head(sf, n = 5)
#' head(sa)
#' head(sa, n = 5)
#' @examples
#' sf <- sframe(id = 1:100, y = rnorm(100))
#' head(sf, 5)
setMethod("head",
          signature = c("sframe"),
          function(x, ...) {
            args <- as.list(match.call())
            if (length(args) == 2) {
              n = 10
            } else {
              n = args[[3]]
            }
            xptr <- x@backend$head(n)
            sfModule <- new(gl_sframe, xptr)
            return(
              new(
                "sframe", backend = sfModule, names = sfModule$colnames(), pointer = sfModule$get()
              )
            )
          })

#' Last part of a sframe
#' @name tail
#' @aliases tail,sframe-method, tail,sarray-method
#' @param x a sframe
#' @param n number of rows to show. Default value is 10
#' @export
#' @usage tail(sf)
#' tail(sf, n = 5)
#' tail(sa)
#' tail(sa, n = 5)
#' @examples
#' sf <- sframe(id = 1:100, y = rnorm(100))
#' tail(sf, 5)
setMethod("tail",
          signature = c("sframe"),
          function(x, ...) {
            args <- as.list(match.call())
            if (length(args) == 2) {
              n = 10
            } else {
              n = args[[3]]
            }
            xptr <- x@backend$tail(n)
            sfModule <- new(gl_sframe, xptr)
            return(
              new(
                "sframe", backend = sfModule, names = sfModule$colnames(), pointer = sfModule$get()
              )
            )
          })

#' Coerce a sframe into a data.frame
#' @name as.data.frame
#' @aliases as.data.frame,sframe,missing,missing-method
#' @usage as.data.frame(x)
#' @param x a sframe
#' @return a data.frame
#' @export
#' @examples
#' sf <- sframe(id = 1:100, y = rnorm(100))
#' as.data.frame(sf)
setMethod("as.data.frame",
          signature = c("sframe", "missing", "missing"),
          function(x, row.names = NULL,  optional = FALSE, ...) {
            return(x@backend$to_dataframe())
          })

#' Extract or Replace Parts of a sframe, sarray, dict
#' @name Extract
#' @rdname Extract
#' @aliases [ [<- [,sframe,ANY,missing,missing-method [<-,sframe,ANY,missing-method
#'          [ [<- [,dict,ANY,missing,missing-method [<-,dict,ANY,missing-method
#'          [ [,sarray,ANY,missing,missing-method
#' @usage x[i]
#' @param x a sframe, sarray, dict
#' @param i column name in sframe, index in sarray, key in dict
#' @export "["
#' @export "[<-"
#' @export "$"
#' @examples
#' sf <- sframe(id = 1:100, y = rnorm(100), x = rlnorm(100))
#' sf['id']
#' sf['x'] <- sarray(1:100)
#' sf[1]
#' sf[c(1,3)]
setMethod("[", signature("sframe", "ANY", "missing", "missing"),
          function (x, i, j, ..., drop) {
            if (is.sarray(i)) {
              xptr <- x@backend$logical_filter(i@backend$get())
              sfModule <- new(gl_sframe, xptr)
              return(
                new(
                  "sframe", backend = sfModule, names = sfModule$colnames(), pointer = sfModule$get()
                )
              )
            } else if (typeof(i) == "character") {
              if (length(i) > 1) {
                xptr <- x@backend$select_columns(i)
                sfModule <- new(gl_sframe, xptr)
                return(
                  new(
                    "sframe", backend = sfModule, names = sfModule$colnames(), pointer = sfModule$get()
                  )
                )
              } else {
                xptr <- x@backend$select_one_column(i)
                sfModule <- new(gl_sarray, xptr)
                return(new("sarray", backend = sfModule, pointer = sfModule$get()))
              }
            } else {
              col_names = colnames(x)
              sel_names = col_names[i]
              if (length(sel_names) > 1) {
                xptr <- x@backend$select_columns(sel_names)
                sfModule <- new(gl_sframe, xptr)
                return(
                  new(
                    "sframe", backend = sfModule, names = sfModule$colnames(), pointer = sfModule$get()
                  )
                )
              } else {
                xptr <- x@backend$select_one_column(sel_names)
                sfModule <- new(gl_sarray, xptr)
                return(new("sarray", backend = sfModule, pointer = sfModule$get()))
              }
            }
          })

setMethod("$", signature = c(x = "sframe"),
          function(x, name) {
            xptr <- x@backend$select_one_column(name)
            sfModule <- new(gl_sarray, xptr)
            return(new("sarray", backend = sfModule, pointer = sfModule$get()))
          })

## used for sf['x']<-, the same with setMethod("[<-"
setMethod("[<-", c("sframe", "ANY", "missing", "ANY"),
          function(x, i, j, ..., value = value) {
            x@backend$add_column(value@backend$get(), i)
            xptr <- x@backend$get()
            sfModule <- new(gl_sframe, xptr)
            return(
              new(
                "sframe", backend = sfModule, names = sfModule$colnames(), pointer = sfModule$get()
              )
            )
          })


#' A sframe object or not
#' @param x an object to test
#' @usage is.sframe(x)
#' @export
is.sframe <- function(x)
  inherits(x, "sframe")

#' Sample the current sframe's rows.
#' @name sample_frac
#' @rdname sample_frac-sframe
#' @aliases sample_frac,sframe-method
#' @usage sample_frac(x, 0.1)
#' @param x a sframe
#' @param fraction approximate fraction of the rows to fetch. Must be between 0 and 1.
#'        The number of rows returned is approximately the fraction times the number of rows.
#' @param seed random seed
#' @export
setMethod("sample_frac",
          signature = c(x = "sframe"),
          function(x, fraction, seed = 123) {
            xptr <- x@backend$sample(fraction, seed)
            sfModule <- new(gl_sframe, xptr)
            return(
              new(
                "sframe", backend = sfModule, names = sfModule$colnames(), pointer = sfModule$get()
              )
            )
          })

setMethod("cbind2",
          signature = c(x = "sframe", y = "sframe"),
          function(x, y)
            cbind(x, y))

#' Combine sframes by column
#' @name cbind
#' @param \code{...} sframes to combine
#' @usage cbind(sf1, sf2)
#' @export
#' @examples
#' \dontrun{
#' cbind(sf1, sf2)
#' }
cbind.sframe <- function(..., deparse.level = 1) {
  dots <- list(...)
  sf1 <- dots[[1]]
  sf2 <- dots[[2]]
  
  col_names1 <- sf1@backend$colnames()
  col_names2 <- sf2@backend$colnames()
  if (length(unique(c(col_names1, col_names2))) < length(col_names1) + length(col_names2)) {
    stop("There are duplicate column names between two sframe")
  }
  xptr <- sf1@backend$cbind(sf2@backend$get())
  sfModule <- new(gl_sframe, xptr)
  return(
    new(
      "sframe", backend = sfModule, names = sfModule$colnames(), pointer = sfModule$get()
    )
  )
  
}

setMethod("rbind2",
          signature = c(x = "sframe", y = "sframe"),
          function(x, y)
            cbind(x, y))

#' Combine sframes by row
#' @name rbind
#' @aliases rbind.sframe
#' @param \code{...} sframes to combine
#' @export
#' @usage rbind(sf1, sf2)
rbind.sframe <- function(..., deparse.level = 1) {
  dots <- list(...)
  sf1 <- dots[[1]]
  sf2 <- dots[[2]]
  
  if (sf1@backend$ncol() != sf2@backend$ncol()) {
    stop("Two sframes have different number of columns")
  }
  
  col_names1 <- sf1@backend$colnames()
  col_names2 <- sf2@backend$colnames()
  if (sum(col_names1 == col_names2) < length(col_names1)) {
    stop("Two sframes have different column names")
  }
  xptr <- sf1@backend$rbind(sf2@backend$get())
  sfModule <- new(gl_sframe, xptr)
  return(
    new(
      "sframe", backend = sfModule, names = sfModule$colnames(), pointer = sfModule$get()
    )
  )
}

#' Remove duplicated rows from sframe
#' @name unique
#' @rdname unique-sframe
#' @aliases unique,sframe-method
#' @export
#' @usage unique(x)
#' @param x a sframe
setMethod("unique",
          signature = c(x = "sframe"),
          function(x, incomparables = FALSE, ...) {
            xptr <- x@backend$unique()
            sfModule <- new(gl_sframe, xptr)
            return(
              new(
                "sframe", backend = sfModule, names = sfModule$colnames(), pointer = sfModule$get()
              )
            )
          })

#' Apply a lambda function over a sframe or a sarray
#'
#' @param s a sframe or a sarray
#' @param FUN a lambda function applied on each row in a sframe or each element in a sarray
#' @return a sarray with each element as the result of the lambda function
#' @details When defining the lambda function for sframe, the function should take a row as input,
#'          which is a list. Calling external functions in other package in lambda fuction
#'          is allowed.
#' @export
#' @examples
#' \dontrun{
#' library(mvtnorm)
#' sf <- sframe(id = 1:100, y = rnorm(100))
#' test <- function(x) {qmvt( dnorm(x[['y']], sd = 16))}
#' sfapply(sf, test)
#' }
sfapply <- function (s, FUN) {
  
  if (!is.sframe(s) && !is.sarray(s)) {
    stop("sfapply method is only available for sframe and sarray")
  }
  
  funlst <- .funlst(deparse(substitute(FUN)))
  arg <- .get_pkg(funlst)
  funlst <- lapply(arg$fun, FUN = match.fun)
  # help R to find rcpplambda_workers
  lib_path <-
    .libPaths()[file.exists(file.path(.libPaths(), "sframe"))][1]
  
  if (is.null(arg$pkg)) {
    xptr <- s@backend$apply(funlst, arg$fun, c(""), lib_path)
  } else {
    xptr <- s@backend$apply(funlst, arg$fun, arg$pkg, lib_path)
  }
  
  glModule <- new(gl_sarray, xptr)
  return(new("sarray", backend = glModule, pointer = glModule$get()))
}

#' Rename colunms of a sframe
#'
#' @param sf a sframe
#' @param old_names the old column names
#' @param new_names the new column names, it should be the same length with old_names
#' @export
#' @usage rename(sf, old_names, new_names)
rename <- function(sf, old_names, new_names) {
  if (length(old_names) != length(new_names)) {
    stop("Number of old and new names don't match!")
  }
  sf@backend$rename(old_names, new_names)
}

#' The sframe is empty or not
#'
#' @param sf a sframe
#' @return \code{TRUE} if the sframe is empty
#' @export
is.empty <- function(sf) {
  return(sf@backend$empty())
}

#' Randomly split the rows of a sframe into two sframes.
#'
#' @param sf a sframe
#' @param fraction Approximate fraction of the rows to fetch for the first returned
#'        sframe. Must be between 0 and 1.
#' @param seed random seed
#' @return a \code{list} of two \code{sframe}s
#' @export
#' @details The first sframe contains M rows, sampled uniformly (without replacement) from the
#'          original sframe. M is approximately the fraction times the original number of rows.
#'          The second sframe contains the remaining rows of the original sframe.
#'
random_split <- function(sf, fraction, seed = 123) {
  if (fraction > 1 || fraction < 0) {
    stop("fraction should be between 0 and 1!")
  }
  l <- sf@backend$random_split(fraction, seed)
  return(list(
    "sframe1" = new(
      gl_sframe, backend = l$sframe1, names = l$sframe1$colnames(), pointer = l$sframe1$get()
    ),
    "sframe2" = new(
      gl_sframe, backend = l$sframe2, names = l$sframe2$colnames(), pointer = l$sframe2$get()
    )
  ))
}

#' The top k rows by one column
#'
#' @param sf a sframe
#' @param colname the column name
#' @param seed random seed
#' @param size k
#' @param reverse, reversed order or not
#' @return a new \code{sframe}
#' @export
topk <- function(sf, colname, size = 10, reverse = FALSE) {
  xptr <- sf@backend$topk(colname, size, reverse)
  sfModule <- new(gl_sframe, xptr)
  return(
    new(
      "sframe", backend = sfModule, names = sfModule$colnames(), pointer = sfModule$get()
    )
  )
}

#' Remove one column from the sframe
#'
#' @param sf a sframe
#' @param colname the column to remove
#' @export
rm.col <- function(sf, colname) {
  sf@backend$remove_column(colname)
}

#' Sort the sframe by one column
#'
#' @param sf a sframe
#' @param colname the column name
#' @param ascending ascending or descending
#' @return the sorted \code{sframe}
#' @export
#' @examples
#' sf <- sframe(id = c(1:50, 1:50), y = rnorm(100))
#' sort_by(sf, "id")
sort_by <- function(sf, colname, ascending = TRUE) {
  xptr <- sf@backend$sortby(colname, ascending)
  sfModule <- new(gl_sframe, xptr)
  return(
    new(
      "sframe", backend = sfModule, names = sfModule$colnames(), pointer = sfModule$get()
    )
  )
}

#' Remove all NA values from a sframe
#'
#' @param sf a sframe
#' @param colname the column name
#' @param how whether a row should be dropped if at least one column has missing values, or if all columns have missing values.
#' @return the \code{sframe}
#' @export
dropna <- function(sf, colname, how = "any") {
  xptr <- sf@backend$dropna(colname, how)
  sfModule <- new(gl_sframe, xptr)
  return(
    new(
      "sframe", backend = sfModule, names = sfModule$colnames(), pointer = sfModule$get()
    )
  )
}

#' Perform a group on the key columns followed by aggregations on the columns
#' listed in operations
#'
#' @param sf a sframe
#' @param group_keys column(s) to group by. Key columns can be of any type other
#'        than dictionary.
#' @param \code{...} Aggregation operations. \code{mean}, \code{std}, \code{sum},
#'                   \code{max}, \code{min}, \code{count}, \code{var}, \code{select},
#'                   \code{cat}, \code{quantile}, \code{argmax}, \code{argmin}
#'                   are support. See details.
#'
#' @details You can use the aggregation operation by
#'          \code{group_by(sf, "id", mean_y = mean(y))}, which means column "id" is
#'          the column to group by and \code{mean} operation is used on column "y",
#'          then a new column named 'mean_y' will be generated in the returned sframe.
#'
#' @return a sframe with a column for each groupby column and each aggregation
#'         operation
#' @export
#' @examples
#' sf <- sframe(id = c(1:50, 1:50), y = rnorm(100))
#' group_by(sf, "id", mean_y = mean(y))
group_by <- function(sf, group_keys, ...) {
  group_by_desc = lazyeval::lazy_dots(...)
  lst <- list()
  for (i in 1:length(group_by_desc)) {
    lst[[i]] <- all.names(group_by_desc[[i]]$expr)
  }
  support_fun <-
    c(
      "mean", "std", "sum", "max", "min", "count", "var",
      "select", "cat", "quantile", "argmax", "argmin"
    )
  
  for (i in 1:length(lst)) {
    if (!lst[[i]][1] %in% support_fun) {
      stop(
        "only mean/std/sum/max/min/count/var/select/cat/quantile/argmax/argmin are supported in group by"
      )
    }
  }
  xptr <- sf@backend$group_by(group_keys, names(group_by_desc), lst)
  sfModule <- new(gl_sframe, xptr)
  return(
    new(
      "sframe", backend = sfModule, names = sfModule$colnames(), pointer = sfModule$get()
    )
  )
}

#' Joins two sframe objects. Merges the left sframe with
#' the right sframe using a SQL-style equi-join operation by columns
#' @name join
#' @usage join(x, y, by = "id", how = "inner")
#' @param x the left sframe
#' @param y the right sframe
#' @param by The column name(s) representing the set of join keys.  Each row that
#' has the same value in this set of columns will be merged together.
#' @param  how The type of join to perform.  "inner" is default.
#' @details Four types of join are available for sframe:
#'
#' \code{inner}: Equivalent to a SQL inner join.  Result consists of the
#' rows from the two sframes whose join key values match exactly,
#' merged together into one sframe.
#'
#' \code{left}: Equivalent to a SQL left outer join. Result is the union
#' between the result of an inner join and the rest of the rows from
#' the left sframe, merged with missing values.
#'
#' \code{right}: Equivalent to a SQL right outer join.  Result is the union
#' between the result of an inner join and the rest of the rows from
#' the right sframe, merged with missing values.
#'
#' \code{outer}: Equivalent to a SQL full outer join. Result is
#' the union between the result of a left outer join and a right
#' outer join.
#'
#' @examples
#' animals = sframe(id = c(1, 2, 3, 4), name = c("dog", "cat", "sheep", "cow"))
#' sounds = sframe(id = c(1, 3, 4, 5), sound = c("woof", "baa", "moo", "oink"))
#' join(animals, sounds, by = "id")
#' join(animals, sounds, by = "id", how = "outer")
#' @export
join <- function(x, y, by = NULL, how = "inner") {
  if (is.null(by)) {
    stop("please specify the join keys")
  }
  
  join_type <- c("inner", "left", "right", "outer")
  
  if (!how %in% join_type) {
    stop("not support join type. Only inner, left, right, outer are support.")
  }
  
  xptr <- x@backend$join(y@backend$get(), by, how)
  sfModule <- new(gl_sframe, xptr)
  return(
    new(
      "sframe", backend = sfModule, names = sfModule$colnames(), pointer = sfModule$get()
    )
  )
}

#' Coerce an object into a sframe
#' @param x an object, data.frame, sarray and vector are support
#' @aliases as.sframe.data.frame as.sframe.sarray as.sframe.vector
#' @return a new \code{sframe}
#' @details We don't suport factor type in sframe now.
#' @export as.sframe
#' @export as.sframe.data.frame
#' @export as.sframe.sarray
#' @export as.sframe.vector
as.sframe <- function(x)
{
  UseMethod("as.sframe")
}

as.sframe.data.frame <- function(x) {
  if (is.null(x)) {
    sfModule <- new(gl_sframe)
    return(
      new(
        "sframe", backend = sfModule, names = sfModule$colnames(), pointer = sfModule$get()
      )
    )
  }
  
  sfModule <- new(gl_sframe)
  sfModule$from_dataframe(x)
  return(
    new(
      "sframe", backend = sfModule, names = sfModule$colnames(), pointer = sfModule$get()
    )
  )
}

as.sframe.sarray <- function(x) {
  sfModule <- new(gl_sframe)
  sfModule$from_sarray(x@backend$get())
  return(
    new(
      "sframe", backend = sfModule, names = sfModule$colnames(), pointer = sfModule$get()
    )
  )
}

as.sframe.vector <- function(x) {
  sa <- as.sarray(x)
  as.sframe(sa)
}

#' Construct a sframe from data
#' @param \code{...}
#' @usage sframe(id = 1:2, list = list(c(1:13), c(12,23)))
#' @examples
#' d1 <- dict(a = 1, b =2)
#' d2 <- dict(c = 3, d = 4)
#' sf <- sframe(id = 1:2, dict = c(d1, d2), list = list(c(1:13), c(12,23)))
#' @export
sframe <- function(...)
{
  lst <- list(...)
  if (length(lst) == 1 && class(lst[[1]]) == "data.frame") {
    return(as.sframe(df))
  }
  cn <- names(lst)
  sfModule <- new(gl_sframe)
  sf <-
    new(
      "sframe", backend = sfModule, names = sfModule$colnames(), pointer = sfModule$get()
    )
  for (i in 1:length(lst)) {
    if (class(lst[[i]]) == "dictlist") {
      saModule <- new(gl_sarray)
      sa <-
        new("sarray", backend = saModule, pointer = saModule$get())
      sa@backend$from_dict_list(lst[[i]])
      sf[cn[i]] <- sa
    } else if (class(lst[[i]]) == "list") {
      saModule <- new(gl_sarray)
      sa <-
        new("sarray", backend = saModule, pointer = saModule$get())
      sa@backend$from_list(lst[[i]])
      sf[cn[i]] <- sa
    } else if (class(lst[[i]]) == "integer" ||
               class(lst[[i]]) == "numeric" ||
               class(lst[[i]]) == "character") {
      sf[cn[i]] <- as.sarray(lst[[i]])
    }
  }
  return(sf)
}

#' Expand one column of one sframe to multiple columns with each value in a separate column
#' @param sf a sframe
#' @param colname the column to expand
#' @details A new gl_sframe with the unpacked column replaced with a list of new columns will
#'          be returned. The column must be of list/array/dict type.
#' @examples
#' sf3 = sframe(id = c(1,2,3),
#' wc = c(dict(a = 1), dict(b = 2), dict(a = 1, b = 2)))
#' unpack_columns(sf3, "wc")
#' @export
unpack_columns <- function(sf, colname) {
  xptr <- sf@backend$unpack(colname)
  sfModule <- new(gl_sframe, xptr)
  return(
    new(
      "sframe", backend = sfModule, names = sfModule$colnames(), pointer = sfModule$get()
    )
  )
}

#' Pack two or more columns of one sframe into one single column.
#' @param sf a sframe
#' @param columns the columns to pack, should be a list
#' @param new_column_name the name of new column
#' @param type The type of the resulting column is decided by the type parameter.
#' Allowed values are dict and list.
#' @param fill.na Value to fill into packed column if missing value is encountered.
#' If packing to dictionary, `fill_na` is only applicable to dictionary values;
#' missing keys are not replaced.
#' @details The result is a new sframe with the unaffected columns
#' from the original sframe plus the newly created column.
#'
#' Two types for resulting column are support:
#'
#' \code{dict}: pack to a dictionary SArray where column name becomes dictionary
#' key and column value becomes dictionary value
#'
#' \code{list}: pack all values from the packing columns into a list
#' @examples
#' sf <- sframe(id=1:4, a = c(1, 2, 3, 4), b = c(4, 3, 2, 1))
#' pack_columns(sf, c("a", "b"), "a_b")
#' @export
pack_columns <-
  function(sf, columns, new_column_name, type = "list", fill.na = NA) {
    type_lst <- c("list", "dict")
    if (!type %in% type_lst) {
      stop("only list, vector and dict types are support")
    }
    xptr <- sf@backend$pack(columns, new_column_name, type, fill.na)
    sfModule <- new(gl_sframe, xptr)
    return(
      new(
        "sframe", backend = sfModule, names = sfModule$colnames(), pointer = sfModule$get()
      )
    )
  }

#' Convert a "wide" column of an sframe to one or two "tall" columns by stacking all values
#' @name stack_columns
#' @usage stack_columns(sf, "col", "new_col")
#' @param sf a sframe
#' @param col the column to stack
#' @param new_col the new columns
#' @details The stack works only for columns of list, or array type. One new column is
#' created as a result of stacking, where each row holds one element of the array or
#' list value, and the rest columns from the same original row repeated. The new sframe
#' includes the newly created column and all columns other than the one that is stacked.
#' @examples
#' sf = sframe(topic = c(1,2,3),
#'              friends = list(c(2,3,4), c(5,6), c(4,5,10)))
#' stack_columns(sf, "friends", "friend")
#' @export
stack_columns <- function(sf, col, new_col) {
  xptr <- sf@backend$stack(col, new_col)
  sfModule <- new(gl_sframe, xptr)
  return(
    new(
      "sframe", backend = sfModule, names = sfModule$colnames(), pointer = sfModule$get()
    )
  )
}

#' Concatenate values from one columns into one column, grouping by all other columns.
#' @name unstack_columns
#' @usage unstack_columns(sf, "col", "new_col")
#' @param sf a sframe
#' @param col the column to stack
#' @param new_col the new columns
#' @details The resulting column could be of type list or array. If "column" is a numeric
#' column, the result will be of vector type. If "column" is a non-numeric column,
#' the new column will be of list type.
#' @examples
#' sf = sframe(topic = c(1,2,3),
#' friends = list(c(2,3,4), c(5,6), c(4,5,10)))
#' sf2 = stack_columns(sf, "friends", "friend")
#' unstack_columns(sf2, "friend", "friends")
#' @export
unstack_columns <- function(sf, col, new_col) {
  xptr <- sf@backend$unstack(col, new_col)
  sfModule <- new(gl_sframe, xptr)
  return(
    new(
      "sframe", backend = sfModule, names = sfModule$colnames(), pointer = sfModule$get()
    )
  )
}