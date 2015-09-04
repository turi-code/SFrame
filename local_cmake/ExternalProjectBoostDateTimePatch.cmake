# Build Boost =================================================================
if(EXISTS ${CMAKE_SOURCE_DIR}/deps_version)
        file(READ ${CMAKE_SOURCE_DIR}/deps_version DEPS_VERSION)
else()
        set(DEPS_VERSION "1")
endif()
message(STATUS "Dato deps version ${DEPS_VERSION}")
if (${DEPS_VERSION} LESS 2) 
        message(STATUS "Patching old dato-deps")
        execute_process(COMMAND patch -N -p1 -i ${CMAKE_SOURCE_DIR}/local_cmake/boost_date_time_parser_deps1_to_deps2.patch 
                WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/deps/local/include
                )
endif()
