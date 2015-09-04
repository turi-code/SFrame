##
## Copyright (C) 2015 Dato, Inc.
## All rights reserved.
##
## This software may be modified and distributed under the terms
## of the BSD license. See the LICENSE file for details.

inlineCxxPlugin <- function(...) {
  plugin <- Rcpp::Rcpp.plugin.maker(
    include.before = "#include <DatoCore.hpp>",
    libs = sprintf(
      "%s $(LAPACK_LIBS) $(BLAS_LIBS) $(FLIBS)", sframeLdFlags()
    ),
    LinkingTo      = c("Rcpp", "sframe"),
    package        = "sframe"
  )
  settings <- plugin()
  settings$env$PKG_CXXFLAGS <- CxxFlags()
  settings
}