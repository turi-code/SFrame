##
## Copyright (C) 2015 Dato, Inc.
## All rights reserved.
##
## This software may be modified and distributed under the terms
## of the BSD license. See the LICENSE file for details.

sframe.system.file <- function(...) {
  tools::file_path_as_absolute(base::system.file(..., package = "sframe"))
}

staticLinking <- function() {
  !grepl("^linux", R.version$os)
}

sframeLdPath <- function() {
  if (nzchar(.Platform$r_arch)) {
    ## eg amd64, ia64, mips
    path <- sframe.system.file("lib",.Platform$r_arch)
  } else {
    path <- sframe.system.file("lib")
  }
  path
}

sframeLdFlags <- function(static = staticLinking()) {
  sframedir <- sframeLdPath()
  flags <-
    paste(
      "-L", sframedir, " -lsframe -lunity_shared -lrcpplambda_shared",
      sep = ""
    )
  invisible(flags)
}

CxxFlags <- function() {
  path <- sframe.system.file("include")
  paste("-I", path, " -I", path, "/graphlab", " -std=c++11", sep = "")
}

LdFlags <- function(static = staticLinking()) {
  cat(sframeLdFlags(static = static))
}