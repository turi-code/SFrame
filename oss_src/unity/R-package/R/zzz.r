##
## Copyright (C) 2015 Dato, Inc.
## All rights reserved.
##
## This software may be modified and distributed under the terms
## of the BSD license. See the LICENSE file for details.
##
## zzz.r

## modules to load
loadModule("gl_sframe", TRUE)
loadModule("gl_sarray", TRUE)
loadModule("gl_sgraph", TRUE)
loadModule("gl_dict", TRUE)

## .onAttach will be called when package loaded
## now it only prints simple welcome functions, more things can be done here
.onAttach <- function(lib, pkg) {
  if (interactive()) {
    packageStartupMessage('sframe: scalable machine learning platform from Dato.\n',
                          'http://www.dato.com\n',
                          'Find out more info with\n',
                          'vignette("intro", package="sframe")',
                          domain=NA, appendLF=TRUE)
  }
}
