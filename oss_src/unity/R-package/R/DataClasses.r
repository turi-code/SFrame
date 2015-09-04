##
## Copyright (C) 2015 Dato, Inc.
## All rights reserved.
##
## This software may be modified and distributed under the terms
## of the BSD license. See the LICENSE file for details.
##
## DataClasses.r ---- definition of classes

#' @name sframe-class
#' @docType class
#' @aliases dato-class sframe-class sarray-class sgraph-class dict-class 
#' class:dato class:sframe class:sarray class:sgraph class:dict 
#' Rcpp_gl_dict-class Rcpp_gl_sarray-class Rcpp_gl_sframe-class Rcpp_gl_sgraph-class 
#' gl_dict gl_sarray gl_sframe gl_sgraph
#' @title \code{dato} class and sub-classes
#' @export sframe
#' @exportClass sframe
#' @export gl_sframe
#' @export sarray
#' @exportClass sarray
#' @export gl_sarray
#' @export sgraph
#' @exportClass sgraph
#' @export gl_sgraph
#' @export dict
#' @exportClass dict
#' @export gl_dict
#' @description The class \code{dato} is the main class for the package. 
#' It is a virtual class and thus not supposed to be instanciated
#' directly.
#' 
#' The sub-classes implement specific data structures developed by
#' Dato (\url{https://dato.com/}), including \code{sframe}, \code{sarray} 
#' and \code{dict}.
#' @keywords class
setClass("dato", representation("VIRTUAL"))
setClass("sframe", representation(backend="C++Object", names = "character", pointer = "externalptr"), contains=c("dato"))
setClass("sarray", representation(backend="C++Object", pointer = "externalptr"), contains=c("dato"))
setClass("sgraph", representation(backend="C++Object", pointer = "externalptr"), contains=c("dato"))
setClass("dict", representation(backend="C++Object", pointer = "externalptr"), contains=c("dato"))