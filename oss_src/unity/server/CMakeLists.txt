project(unity)

if(WIN32)
  make_copy_target(unity_core_copy TARGETS ${UNITY_ALL_LIBRARIES})
endif()

make_executable(unity_server
                SOURCES
                  unity_server_init.cpp
                  unity_server.cpp
                REQUIRES
                  unity
                  perf
                  tcmalloc
                  crash_handler
                  minipsutil_static)


add_dependencies(unity_server pylambda_worker rddtosf_pickle rddtosf_nonpickle sftordd_pickle ./oss_src/sframe/graphlab-create-spark-integration.jar)

get_property(EXTENSIONS_LIST DIRECTORY "../extensions" PROPERTY EXTENSIONS_LIST)
foreach(loop_var ${EXTENSIONS_LIST})
  message(STATUS "unity_server Depending on extension ${loop_var}")
  add_dependencies(unity_server ${loop_var})
endforeach()
