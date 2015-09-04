##
## Copyright (C) 2015 Dato, Inc.
## All rights reserved.
##
## This software may be modified and distributed under the terms
## of the BSD license. See the LICENSE file for details.

#' sframe: scalable machine learning platform from Dato
#' 
#' Dato's scalable data frame, the sframe, your data analysis tasks can scale 
#' into the terabyte range, even on your laptop. We try our best to optimize the sframe:
#' 
#' \itemize{
#' \item Memory: A sframe is effectively a data frame that sits on 
#'     your hard disk rather than in memory.
#' \item Parallelization: Custom column transformations are performed using as many 
#'     CPUs as possible. This lets you immediately take full advantage of the hardware you have.
#' \item Group-by, sort, and join: These are done in a way that make intelligent use of memory.
#' \item Sparse data: Check out our stack/unstack feature that lets you take row-based 
#'     lists and concatenate them together into a “long”-format. We've found these extremely 
#'     useful and flexible for feature engineering.
#' }
#' 
#' 
#' @docType package
#' @name sframe
#' @import Rcpp methods
#' @useDynLib libunity_shared 
#' @useDynLib librcpplambda_shared 
#' @useDynLib sframe
NULL