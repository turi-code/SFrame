project(util)

make_library(util SOURCES any.cpp cityhash_gl.cpp syserr_reporting.cpp md5.cpp testing_utils.cpp EXTERNAL_VISIBILITY requires logger)
