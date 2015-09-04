project(serialization)

make_library(serialization
  SOURCES
    dir_archive.cpp
    dir_archive_cache
  REQUIRES
    logger
    fileio
    util
    random
    EXTERNAL_VISIBILITY
  )
