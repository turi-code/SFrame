##
## Copyright (C) 2015 Dato, Inc.
## All rights reserved.
##
## This software may be modified and distributed under the terms
## of the BSD license. See the LICENSE file for details.

#' Create a skeleton for a new package depending on sframe
#'
#' \code{sframe.package.skeleton} automates the creation of
#' a new source package that intends to use features of sframe
#' It is based on the \link[Rcpp]{Rcpp.package.skeleton} function
#' which it executes first.
#' @export
#' @usage
#' sframe.package.skeleton(name = "anRpackage", list = character(),
#' environment = .GlobalEnv, path = ".", force = FALSE,
#' code_files = character(), cpp_files = character(),
#' example_code = TRUE, module = FALSE
#' )
sframe.package.skeleton <-
  function(name = "anRpackage", list = character(),
           environment = .GlobalEnv,
           path = ".", force = FALSE,
           code_files = character(),
           example_code = TRUE) {
    env <- parent.frame(1)
    
    if (!length(list)) {
      fake <- TRUE
      assign("sframe.fake.fun", function() {
        
      }, envir = env)
    } else {
      fake <- FALSE
    }
    
    ## first let the traditional version do its business
    call <- match.call()
    call[[1]] <- as.name("package.skeleton")
    if ("example_code" %in% names(call)) {
      ## remove the example_code argument
      call[["example_code"]] <- NULL
    }
    if (fake) {
      call[["list"]] <- "sframe.fake.fun"
    }
    
    tryCatch(
      eval(call, envir = env),
      error = function(e) {
        stop("error while calling `package.skeleton`")
      }
    )
    
    message("\nAdding sframe settings")
    
    ## now pick things up
    root <- file.path(path, name)
    
    DESCRIPTION <- file.path(root, "DESCRIPTION")
    if (file.exists(DESCRIPTION)) {
      x <- cbind(
        read.dcf(DESCRIPTION),
        "Imports" = sprintf("Rcpp (>= %s)",
                            packageDescription("Rcpp")[["Version"]]),
        "LinkingTo" = "Rcpp, sframe",
        "Depends" = "sframe"
      )
      write.dcf(x, file = DESCRIPTION)
      message(" >> added Imports: Rcpp")
      message(" >> added LinkingTo: Rcpp, sframe")
      message(" >> added Depends: sframe")
    }
    
    ## add a useDynLib to NAMESPACE,
    NAMESPACE <- file.path(root, "NAMESPACE")
    lines <- readLines(NAMESPACE)
    if (!grepl("useDynLib", lines)) {
      lines <- c(sprintf("useDynLib(%s)", name),
                 "importFrom(Rcpp, evalCpp)",        ## ensures Rcpp instantiation
                 lines)
      writeLines(lines, con = NAMESPACE)
      message(" >> added useDynLib and importFrom directives to NAMESPACE")
    }
    
    ## lay things out in the src directory
    src <- file.path(root, "src")
    if (!file.exists(src)) {
      dir.create(src)
    }
    skeleton <- system.file("skeleton", package = "sframe")
    Makevars <- file.path(src, "Makevars")
    if (!file.exists(Makevars)) {
      file.copy(file.path(skeleton, "Makevars"), Makevars)
      message(" >> added Makevars file with sframe settings")
    }
    
    if (example_code) {
      file.copy(file.path(skeleton, "weighted_pagerank.cpp"), src)
      message(" >> added example")
      compileAttributes(root)
      message(" >> invoked Rcpp::compileAttributes to create wrappers")
    }
    
    if (fake) {
      rm("sframe.fake.fun", envir = env)
      unlink(file.path(root, "R"  , "sframe.fake.fun.R"))
      unlink(file.path(root, "man", "sframe.fake.fun.Rd"))
    }
    
    invisible(NULL)
  }
