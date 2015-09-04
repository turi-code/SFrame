##
## Copyright (C) 2015 Dato, Inc.
## All rights reserved.
##
## This software may be modified and distributed under the terms
## of the BSD license. See the LICENSE file for details.
##
## io.r ---- R functions related to input/output of sframe

#' Load a sframe
#'
#' @param filename Location of the file to load. Can be a local path or a remote URL.
#' @return a new \code{sframe}
#'
#' @details The filename extension is used to determine the format
#' automatically. This function is particularly useful for sframes previously
#' saved in binary format. For CSV imports the \code{load.csv} function
#' provides greater control. If the sframe is in binary format, \code{filename} is
#' actually a directory, created when the sframe is saved.
#' @export
#' @examples
#' \dontrun{
#' sf <- load.sframe("saved_sframe")
#' }
load.sframe <- function(filename) {
  filename <- path.expand(filename)
  sfModule <- new(gl_sframe)
  sfModule$load(filename)
  return(
    new(
      "sframe", backend = sfModule, names = sfModule$colnames(), pointer = sfModule$get()
    )
  )
}

#' Load a sgraph
#'
#' @param filename Location of the file to load. Can be a local path or a remote URL.
#' @return a new \code{sgraph}
#' @export
#' @examples
#' \dontrun{
#' sg <- load.sgraph("saved_sgraph")
#' }
load.sgraph <- function(filename) {
  filename <- path.expand(filename)
  sgModule <- new(gl_sgraph)
  sgModule$load(filename)
  return(new("sgraph", backend = sgModule, pointer = sgModule$get()))
}

#' Create a new SFrame from a csv file
#'
#' @param filename the name of the file which the data are to be read from.
#'        Each row of the table appears as one line of the file. If it does
#'        not contain an absolute path, the file name is relative to the
#'        current working directory. Tilde-expansion is performed where supported.
#'        This can be a local path or a remote URL.
#' @param sep the field separator character. Values on each line of the file are
#'        separated by this character. Tab-sep is the default value.
#' @param header a logical value indicating whether the file contains the names of
#'        the variables as its first line.
#' @param cols the specific columns to read. If empty, all columns will be read.
#' @param na.strings a character vector of strings which are to be interpreted as
#'        NA values.
#' @param error_bad_lines if true, will fail upon encountering a bad line.
#' @param comment.char a character vector of length one containing a single
#'        character or an empty string.
#' @param terminator a string to be interpreted as the line terminator
#' @param nrow number of rows to read
#' @param colClasses used to specify the class of each row, default value is NA,
#'        which means the class of each row will determined from data
#' @return a new \code{sframe}
#' @export
#' @examples
#' \dontrun{
#' sf <- load.csv("diamonds.csv", header = TRUE, sep = ",")
#' }
load.csv <-
  function(filename, sep = "\t", header = FALSE, cols = "", na.strings = "",
           error_bad_lines = TRUE, comment.char = "", terminator = "\n", nrow = "",
           colClasses = NA) {
    filename <- path.expand(filename)
    
    if (nrow == "") {
      opt_keys <-
        c(
          "delimiter", "use_header", "output_columns", "na_values",
          "error_bad_lines", "comment_char", "line_terminator"
        )
      
      opt_values <-
        list(
          sep, header, "output_columns" = cols, "na_values" = na.strings,!error_bad_lines, comment.char, terminator
        )
    } else if (nrow < 0) {
      stop("number of rows must be positive!")
    } else {
      opt_keys <-
        c(
          "delimiter", "use_header", "output_columns", "na_values",
          "error_bad_lines", "comment_char", "line_terminator", "row_limit"
        )
      
      opt_values <-
        list(
          sep, header, "output_columns" = cols, "na_values" = na.strings,!error_bad_lines, comment.char, terminator, nrow
        )
    }
    
    sfModule <- new(gl_sframe)
    
    col_type = vector()
    
    lines <- readLines(filename, 10)
    lines <- gsub(sep, ",", lines)
    tmp <-
      read.table(
        text = lines, header = header, sep = ",", comment.char = comment.char,
        stringsAsFactors = FALSE, colClasses = colClasses
      )
    
    for (i in 1:length(tmp[1,])) {
      if (is.integer(tmp[1, i])) {
        col_type = append(col_type, "integer")
      } else if (is.double(tmp[1, i])) {
        col_type = append(col_type, "double")
      } else if (is.character(tmp[1, i])) {
        col_type = append(col_type, "character")
      } else {
        stop("Column type not supported.")
      }
    }
    
    if (header) {
      column_name = colnames(tmp)
    } else {
      column_name = vector()
      for (i in 1:ncol(tmp)) {
        column_name = append(column_name, paste("X", i, sep = ""))
      }
    }
    
    sfModule$load_from_csvs(filename, opt_keys, opt_values, column_name, col_type)
    return(
      new(
        "sframe", backend = sfModule, names = sfModule$colnames(),pointer = sfModule$get()
      )
    )
  }

#' Save the sframe to a file system for later use.
#' @param x the sframe to save
#' @param filename The location to save the SFrame. Either a local directory or a
#'        remote URL. If the format is 'binary', a directory will be created
#'        at the location which will contain the sframe.
#' @param format 'binary' or 'csv'. Format in which to save the SFrame. Binary
#'        saved SFrames can be loaded much faster and without any format conversion
#'        losses. If not given, will try to infer the format from filename given.
#' @export
#' @examples
#' \dontrun{
#' save.sframe(sf)
#' }
save.sframe <-
  function(x, filename = paste("sframe", Sys.Date(), sep = "-"), format = "binary") {
    if (!is.sframe(x)) {
      stop(paste(x, " is not a sframe", sep = ""))
    }
    
    if (format != "binary" && format != "csv") {
      stop("Only binary and csv formats are supported.")
    }
    
    filename <- path.expand(filename)
    x@backend$save(filename, format)
  }

#' Save the sgraph to a file system for later use.
#' @param x the sgraph to save
#' @param filename The location to save the SFrame. Either a local directory or a
#'        remote URL.A directory will be created at the location which will contain the sframe.
#' @export
#' @examples
#' \dontrun{
#' save.sgraph(sg)
#' }
save.sgraph <-
  function(x, filename = paste("sgraph", Sys.Date(), sep = "-")) {
    if (!is.sgraph(x)) {
      stop(paste(x, " is not a sframe", sep = ""))
    }
    filename <- path.expand(filename)
    x@backend$save(filename)
  }

#' Save a sarray into file
#' @param x a sarray
#' @param filename A local path or a remote URL.  If format is 'text', it will be
#'        saved as a text file. If format is 'binary', a directory will be created
#'        at the location which will contain the SArray.
#' @param format 'binary', 'text', 'csv'. Format in which to save the SFrame.
#'        Binary saved SArrays can be loaded much faster and without any format
#'        conversion losses. 'text' and 'csv' are synonymous: Each SArray row will
#'        be written as a single line in an output text file. If not given, will try
#'        to infer the format from filename given.
#' @export
#' @examples
#' \dontrun{
#' save.sarray(sa)
#' }
save.sarray <-
  function(x, filename = paste("sarray", Sys.Date(), sep = "-"), format = "binary") {
    if (!is.sarray(x)) {
      stop(paste(x, " is not a sarray", sep = ""))
    }
    
    if (format != "binary" && format != "csv") {
      stop("Only binary and csv formats are support.")
    }
    filename <- path.expand(filename)
    x@backend$save(filename, format)
  }