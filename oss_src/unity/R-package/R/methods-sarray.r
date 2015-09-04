##
## Copyright (C) 2015 Dato, Inc.
## All rights reserved.
##
## This software may be modified and distributed under the terms
## of the BSD license. See the LICENSE file for details.
##
## methods-sarray.r ---- R functions related to sarray

## In R console, when typing the name of one object
## the show() function will be called automatically
setMethod("show",
          signature = c("sarray"),
          function(object) {
            object@backend$show()
          })

## as.vector(sa)
#' @export "as.vector"
setMethod("as.vector",
          signature = c("sarray"),
          function(x, mode = "any") {
            return(x@backend$to_vec())
          })

setMethod("[", signature("sarray", "ANY", "missing", "missing"),
          function (x, i, j, ..., drop) {
            if (i <= 0) {
              stop("sarray index begins from 1")
            }
            if (class(x@backend$at(i - 1)) == "externalptr") {
              mod <- new(gl_dict, x@backend$at(i - 1))
              return(new("dict", backend = mod, pointer = mod$get()))
            } else
              return(x@backend$at(i - 1))
          })

setMethod("length",
          signature = c("sarray"),
          function(x)
            return(x@backend$length()))

## head(sa, n = 10)
setMethod("head",
          signature = c("sarray"),
          function(x, ...) {
            args <- as.list(match.call())
            if (length(args) == 2) {
              n = 10
            } else {
              n = args[[3]]
            }
            xptr <- x@backend$head(n)
            sfModule <- new(gl_sarray, xptr)
            return(new("sarray", backend = sfModule, pointer = sfModule$get()))
          })

## tail(sa, n = 10)
setMethod("tail",
          signature = c("sarray"),
          function(x, ...) {
            args <- as.list(match.call())
            if (length(args) == 2) {
              n = 10
            } else {
              n = args[[3]]
            }
            xptr <- x@backend$tail(n)
            sfModule <- new(gl_sarray, xptr)
            return(new("sarray", backend = sfModule, pointer = sfModule$get()))
          })

#' Arithmetic operations on sarrays
#' @name Arithmetic
#' @rdname arithmetic-sarray
#' @aliases min max sum mean sd `+.sarray` `-.sarray` `*.sarray` `/.sarray`
#' @export min 
#' @export max 
#' @export sum 
#' @export mean 
#' @export sd 
#' @export "+.sarray" 
#' @export "-.sarray" 
#' @export "*.sarray" 
#' @export "/.sarray"
#' @usage min(sa)
#' max(sa)
#' mean(sa)
#' sd(sa)
#' sa1 + sa2
#' sa1 - sa2
#' sa1 * sa2
#' sa1 / sa2
setMethod("min",
          signature = c(x = "sarray"),
          function(x, ..., na.rm = FALSE) {
            return(x@backend$min(na.rm))
          })

setMethod("max",
          signature = c(x = "sarray"),
          function(x, ..., na.rm = FALSE) {
            return(x@backend$max(na.rm))
          })

setMethod("sum",
          signature = c(x = "sarray"),
          function(x, ..., na.rm = FALSE) {
            return(x@backend$sum(na.rm))
          })

setMethod("mean",
          signature = c(x = "sarray"),
          function(x, ...) {
            return(x@backend$mean())
          })

setMethod("sd",
          signature = c(x = "sarray"),
          function(x, na.rm = FALSE) {
            return(x@backend$std(na.rm))
          })

#' Remove duplicated elements from sarray
#' @name unique
#' @rdname unique-sarray
#' @aliases unique,sarray-method
#' @usage unique(x)
#' @param x a sarray
#' @export
setMethod("unique",
          signature = c(x = "sarray"),
          function(x, incomparables = FALSE, ...) {
            xptr <- x@backend$unique()
            sfModule <- new(gl_sarray, xptr)
            return(new("sarray", backend = sfModule, pointer = sfModule$get()))
          })

#' Sorting sarrays
#' @usage sort(sa)
#' @aliases sort,sarray-method
#' @export
setMethod("sort",
          signature = c(x = "sarray"),
          function(x, decreasing = FALSE, ...) {
            xptr <- x@backend$sort(decreasing)
            sfModule <- new(gl_sarray, xptr)
            return(new("sarray", backend = sfModule, pointer = sfModule$get()))
          })
#' Sample the current sarray's elements
#' @name sample_frac
#' @aliases sample_frac,sarray-method
#' @rdname sample_frac-sarray
#' @usage sample_frac(x, 0.1)
#' @export
setMethod("sample_frac",
          signature = c(x = "sarray"),
          function(x, fraction, seed = 123) {
            xptr <- x@backend$sample(fraction, seed)
            sfModule <- new(gl_sarray, xptr)
            return(new("sarray", backend = sfModule, pointer = sfModule$get()))
          })

#' Reports whether x is a sarray object
#' @param x An object to test
#' @export
is.sarray <- function(x)
  inherits(x, "sarray")


#' Convert a vector into a sarray
#'
#' @param x a vector
#' @return a new \code{sarray}
#' @export
as.sarray <- function(x) {
  if (is.null(x)) {
    sfModule <- new(gl_sarray)
    return(new("sarray", backend = sfModule, pointer = sfModule$get()))
  }
  
  sfModule <- new(gl_sarray)
  sfModule$from_vec(x)
  return(new("sarray", backend = sfModule, pointer = sfModule$get()))
}

#' Construct a sarray from data
#' @param \code{...}
#' @usage sarray(1:10000)
#' @export
#' @examples
#' sa <- sarray(1:10000)
sarray <- function(...) {
  x <- unlist(list(...))
  if (is.null(x)) {
    sfModule <- new(gl_sarray)
    return(new("sarray", backend = sfModule, pointer = sfModule$get()))
  }
  if (length(x) == 1) {
    return(as.array(x))
  }
  sfModule <- new(gl_sarray)
  sfModule$from_vec(x)
  return(new("sarray", backend = sfModule, pointer = sfModule$get()))
}

`+.sarray` <- function(s1, s2) {
  if (is.sarray(s1) && is.sarray(s2)) {
    xptr <- s1@backend$op_add(s2@backend$get())
    sfModule <- new(gl_sarray, xptr)
    return(new("sarray", backend = sfModule, pointer = sfModule$get()))
  } else if (is.sarray(s1)) {
    xptr <- s1@backend$op_add(s2)
    sfModule <- new(gl_sarray, xptr)
    return(new("sarray", backend = sfModule, pointer = sfModule$get()))
  } else {
    xptr <- s2@backend$op_add(s1)
    sfModule <- new(gl_sarray, xptr)
    return(new("sarray", backend = sfModule, pointer = sfModule$get()))
  }
}

`-.sarray` <- function(s1, s2) {
  if (is.sarray(s1) && is.sarray(s2)) {
    xptr <- s1@backend$op_minus(s2@backend$get())
    sfModule <- new(gl_sarray, xptr)
    return(new("sarray", backend = sfModule, pointer = sfModule$get()))
  } else if (is.sarray(s1)) {
    xptr <- s1@backend$op_minus(s2)
    sfModule <- new(gl_sarray, xptr)
    return(new("sarray", backend = sfModule, pointer = sfModule$get()))
  } else {
    xptr <- s2@backend$minus(s1)
    sfModule <- new(gl_sarray, xptr)
    return(new("sarray", backend = sfModule, pointer = sfModule$get()))
  }
}

`*.sarray` <- function(s1, s2) {
  if (is.sarray(s1) && is.sarray(s2)) {
    xptr <- s1@backend$op_multiply(s2@backend$get())
    sfModule <- new(gl_sarray, xptr)
    return(new("sarray", backend = sfModule, pointer = sfModule$get()))
  } else if (is.sarray(s1)) {
    xptr <- s1@backend$op_multiply(s2)
    sfModule <- new(gl_sarray, xptr)
    return(new("sarray", backend = sfModule, pointer = sfModule$get()))
  } else {
    xptr <- s2@backend$op_multiply(s1)
    sfModule <- new(gl_sarray, xptr)
    return(new("sarray", backend = sfModule, pointer = sfModule$get()))
  }
}

`/.sarray` <- function(s1, s2) {
  if (is.sarray(s1) && is.sarray(s2)) {
    xptr <- s1@backend$op_divide(s2@backend$get())
    sfModule <- new(gl_sarray, xptr)
    return(new("sarray", backend = sfModule, pointer = sfModule$get()))
  } else if (is.sarray(s1)) {
    xptr <- s1@backend$op_divide(s2)
    sfModule <- new(gl_sarray, xptr)
    return(new("sarray", backend = sfModule, pointer = sfModule$get()))
  } else {
    stop("operation not support!")
  }
}

#' Relational operators over sarrays
#' @rdname relational-sarray
#' @name Comparison
#' @aliases `==.sarray` `<.sarray` `<=.sarray` `>.sarray` `>=.sarray`
#' @export "==.sarray"
#' @export "<.sarray"
#' @export "<=.sarray"
#' @export ">.sarray" 
#' @export ">=.sarray"
#' @usage sa1 == sa2
#' sa1 > sa2
#' sa1 >= sa2
#' sa1 < sa2
#' sa1 <= sa2
`==.sarray` <- function(s1, s2) {
  if (is.sarray(s1) && is.sarray(s2)) {
    xptr <- s1@backend$op_equal(s2@backend$get())
    sfModule <- new(gl_sarray, xptr)
    return(new("sarray", backend = sfModule, pointer = sfModule$get()))
  } else if (is.sarray(s1)) {
    xptr <- s1@backend$op_equal(s2)
    sfModule <- new(gl_sarray, xptr)
    return(new("sarray", backend = sfModule, pointer = sfModule$get()))
  } else {
    xptr <- s2@backend$op_equal(s1)
    sfModule <- new(gl_sarray, xptr)
    return(new("sarray", backend = sfModule, pointer = sfModule$get()))
  }
}

`<.sarray` <- function(s1, s2) {
  if (is.sarray(s1) && is.sarray(s2)) {
    xptr <- s1@backend$op_less(s2@backend$get())
    sfModule <- new(gl_sarray, xptr)
    return(new("sarray", backend = sfModule, pointer = sfModule$get()))
  } else if (is.sarray(s1)) {
    xptr <- s1@backend$op_less(s2)
    sfModule <- new(gl_sarray, xptr)
    return(new("sarray", backend = sfModule, pointer = sfModule$get()))
  } else {
    xptr <- s2@backend$op_greater(s1)
    sfModule <- new(gl_sarray, xptr)
    return(new("sarray", backend = sfModule, pointer = sfModule$get()))
  }
}

`<=.sarray` <- function(s1, s2) {
  if (is.sarray(s1) && is.sarray(s2)) {
    xptr <- s1@backend$op_leq(s2@backend$get())
    sfModule <- new(gl_sarray, xptr)
    return(new("sarray", backend = sfModule, pointer = sfModule$get()))
  } else if (is.sarray(s1)) {
    xptr <- s1@backend$op_leq(s2)
    sfModule <- new(gl_sarray, xptr)
    return(new("sarray", backend = sfModule, pointer = sfModule$get()))
  } else {
    xptr <- s2@backend$op_geq(s1)
    sfModule <- new(gl_sarray, xptr)
    return(new("sarray", backend = sfModule, pointer = sfModule$get()))
  }
}

`>.sarray` <- function(s1, s2) {
  if (is.sarray(s1) && is.sarray(s2)) {
    xptr <- s1@backend$op_greater(s2@backend$get())
    sfModule <- new(gl_sarray, xptr)
    return(new("sarray", backend = sfModule, pointer = sfModule$get()))
  } else if (is.sarray(s1)) {
    xptr <- s1@backend$op_greater(s2)
    sfModule <- new(gl_sarray, xptr)
    return(new("sarray", backend = sfModule, pointer = sfModule$get()))
  } else {
    xptr <- s2@backend$op_less(s1)
    sfModule <- new(gl_sarray, xptr)
    return(new("sarray", backend = sfModule, pointer = sfModule$get()))
  }
}

`>=.sarray` <- function(s1, s2) {
  if (is.sarray(s1) && is.sarray(s2)) {
    xptr <- s1@backend$op_geq(s2@backend$get())
    sfModule <- new(gl_sarray, xptr)
    return(new("sarray", backend = sfModule, pointer = sfModule$get()))
  } else if (is.sarray(s1)) {
    xptr <- s1@backend$op_geq(s2)
    sfModule <- new(gl_sarray, xptr)
    return(new("sarray", backend = sfModule, pointer = sfModule$get()))
  } else {
    xptr <- s2@backend$op_leq(s1)
    sfModule <- new(gl_sarray, xptr)
    return(new("sarray", backend = sfModule, pointer = sfModule$get()))
  }
}

#' Logical operators over sarrays
#' @rdname logical-sarray
#' @name Logic
#' @aliases `&.sarray` `|.sarray`
#' @export "&.sarray" 
#' @export "|.sarray"
#' @usage sa1 | sa2
#' sa1 & sa2
`&.sarray` <- function(s1, s2) {
  if (is.sarray(s1) && is.sarray(s2)) {
    xptr <- s1@backend$op_and(s2@backend$get())
    sfModule <- new(gl_sarray, xptr)
    return(new("sarray", backend = sfModule, pointer = sfModule$get()))
  } else {
    stop("operation not support!")
  }
}

`|.sarray` <- function(s1, s2) {
  if (is.sarray(s1) && is.sarray(s2)) {
    xptr <- s1@backend$op_or(s2@backend$get())
    sfModule <- new(gl_sarray, xptr)
    return(new("sarray", backend = sfModule, pointer = sfModule$get()))
  } else {
    stop("operation not support!")
  }
}

#' Create a new sarray with all values cast to the given type. 
#' @details 
#' An exception will be thrown if the types are not castable to the given type.
#' @param sa sarray to be cast
#' @param type new type, int, vector, list, dict, double, string types are support
#' @export
astype <- function(sa, type = "int") {
  if (!is.sarray(sa))
    stop("only for sarray type")
  
  type_lst <- c("int", "vector", "list", "dict", "double", "string")
  
  if (!type%in%type_lst) {
    stop("unsupport type.")
  }
  xptr <- sa@backend$astype(type)
  sfModule <- new(gl_sarray, xptr)
  return(new("sarray", backend = sfModule, pointer = sfModule$get()))
}