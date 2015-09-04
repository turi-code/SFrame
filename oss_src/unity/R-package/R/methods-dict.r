##
## Copyright (C) 2015 Dato, Inc.
## All rights reserved.
##
## This software may be modified and distributed under the terms
## of the BSD license. See the LICENSE file for details.
##
## method-dict.r ---- dict type for R

setMethod("show",
          signature(object = "dict"),
          function(object) {
            object@backend$show()
          })

#' Length of a dict object
#' @name length
#' @usage length(x)
#' @aliases length,dict-method
#' @param x a dict object
#' @export
setMethod("length",
          signature = c("dict"),
          function(x)
            return(x@backend$length()))

setMethod("[", signature("dict", "ANY", "missing", "missing"),
          function (x, i, j, ..., drop) {
            x@backend$at(i)
          })

setMethod("[<-", c("dict", "ANY", "missing", "ANY"),
                 function(x, i, j, ..., value = value) {
                   x@backend$set(i, value)
                   return(x)
                 })
#' The dict has the key or not
#' @name %in%
#' @aliases %in%,ANY,dict-method
#' @export
#' @usage x \%in\% d
#' @examples 
#' d <- dict(a = 1, b = 33, c = 7)
#' d
#' "a" %in% d
#' "z" %in% d
setMethod('%in%', 
          signature = c("ANY", "dict"),
          function(x, table) {
            table@backend$has_key(x)
          })

#' Reports whether x is a dict object
#' @param x An object to test
#' @export
is.dict <- function(x)
  inherits(x, "dict")

#' Construct a dict from key-value pairs
#' @param \code{...} key-value pairs
#' @export
#' @examples 
#' d <- dict(a = 1, b = 2)
#' keys(d)
#' values(d)
dict <- function(...) {
  dots <- list(...)
  lst <- as.list(dots)
  mod <- new(gl_dict, lst)
  return(new("dict", backend = mod))
}

#' Get all the keys from the dictionary
#' @param d a \code{dict} object
#' @return a list of keys
#' @export
#' @examples 
#' d <- dict(a = 1, b = 2)
#' keys(d)
keys <- function(d) {
  if (!is.dict(d)) {
    stop("only for dict type")
  }
  return(d@backend$get_keys())
}

#' Get all the values from the dictionary
#' @param d a \code{dict} object
#' @return a list of values
#' @export
#' @examples 
#' d <- dict(a = 1, b = 2)
#' values(d)
values <- function(d) {
  if (!is.dict(d)) {
    stop("only for dict type")
  }
  return(d@backend$get_values())
}

#' Delete one item from the dictionary
#' @param d a \code{dict} object
#' @param key the key to delete
#' @export
#' @examples
#' d <- dict(a = 1, b = 2)
#' d
#' del(d, "a")
#' d
del <- function(d, key) {
  if (!is.dict(d)) {
    stop("only for dict type")
  }
  d@backend$rm(key)
}

#' Combine dictionaries into a Vector
#' @param \code{...} dictionaries to combine
#' @return a \code{dictlist}
#' @export
setMethod("c",
          signature = c(x = "dict"),
          function(x, ..., recursive = FALSE) {
            lst <- list(x, ...)
            res <- list()
            for (i in 1:length(lst)) {
              res[[i]] <- lst[[i]]@backend$get()
            }
            class(res) <- "dictlist"
            res
          })
