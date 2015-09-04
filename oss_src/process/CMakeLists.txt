project(process)

if(WIN32)
  SET(PLATFORM_SOURCES process_win.cpp process_util_win.cpp)
else()
  SET(PLATFORM_SOURCES process_unix.cpp process_util_unix.cpp)
endif()

make_library(process
  SOURCES
    ${PLATFORM_SOURCES}
  REQUIRES
    util
    logger
    boost
)
