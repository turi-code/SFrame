project(zookeeper_util)

# Zookeeper not supported right now because they don't seem to support mingw
if(NOT WIN32)
make_library(zookeeper_util
  SOURCES
  key_value.cpp
  server_list.cpp
  zookeeper_common.cpp
  REQUIRES
   zookeeper
)
endif()
