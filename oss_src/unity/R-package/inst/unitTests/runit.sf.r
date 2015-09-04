##
## Copyright (C) 2015 Dato, Inc.
## All rights reserved.
##
## This software may be modified and distributed under the terms
## of the BSD license. See the LICENSE file for details.
##
## runit.sf.r

.runThisTest <- Sys.getenv("RunAllDatoCoreTests") == "yes"

if (.runThisTest) {

test.sf <- function() {
  df <- data.frame(
    a = c(1, 2, 3),
    b = c("x", "y", "z"),
    c = c(0.1, 0.2, 0.3),
    stringsAsFactors = FALSE
  )
  sf <- as.sframe(df)
  df2 <- as.data.frame(sf)
  checkEquals(df, df2)

  vec <- c(1:15)
  sa <- as.sarray(vec)
  vec2 <- as.vector(sa)
  checkEquals(class(sa)[1], "sarray")
  checkEquals(vec, vec2)
  checkEquals(sa[10], 10)

}

}
