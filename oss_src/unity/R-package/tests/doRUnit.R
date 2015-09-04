##
## Copyright (C) 2015 Dato, Inc.
## All rights reserved.
##
## This software may be modified and distributed under the terms
## of the BSD license. See the LICENSE file for details.
##
## doRUnit.R --- Run RUnit tests

stopifnot(require(RUnit, quietly = TRUE))
stopifnot(require(sframe, quietly = TRUE))

## Set a seed to make the test deterministic
set.seed(42)

## Define tests
testSuite <- defineTestSuite(
  name = "sframe Unit Tests",
  dirs = system.file("unitTests", package = "sframe"),
  testFuncRegexp = "^[Tt]est.+"
)

## without this, we get (or used to get) unit test failures
Sys.setenv("R_TESTS" = "")

if (Sys.getenv("RunAllDatoCoreTests") != "no") {
  # if env.var not yet set
  message("Setting \"RunAllDatoCoreTests\"=\"yes\"\n")
  Sys.setenv("RunAllDatoCoreTests" = "yes")
}

## Run tests
tests <- runTestSuite(testSuite)

## Print results
printTextProtocol(tests)

## Return success or failure to R CMD CHECK
if (getErrors(tests)$nFail > 0) {
  stop("TEST FAILED!")
}
if (getErrors(tests)$nErr > 0) {
  stop("TEST HAD ERRORS!")
}
if (getErrors(tests)$nTestFunc < 1 &&
    Sys.getenv("RunAllRcppTests") == "yes") {
  stop("NO TEST FUNCTIONS RUN!")
}
