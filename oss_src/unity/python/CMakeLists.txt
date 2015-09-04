project(unity)

add_subdirectory(sframe)

# With CMake, a clean separation can be made between the source tree and the
# build tree.  When all source is compiled, as with pure C/C++, the source is
# no-longer needed in the build tree.  However, with pure *.py source, the
# source is processed directly.  To handle this, we reproduce the availability
# of the source files in the build tree.

ADD_CUSTOM_TARGET (oss_python_source ALL)
ADD_CUSTOM_COMMAND(
  COMMENT "build python source"
  COMMAND
  ${CMAKE_COMMAND} -P
  ${CMAKE_SOURCE_DIR}/local_cmake/ReplicatePythonSourceTree.cmake
  ${CMAKE_CURRENT_BINARY_DIR}
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  TARGET oss_python_source
)

ADD_CUSTOM_TARGET (oss_clean_python)
add_dependencies(oss_clean_python oss_clean_graphlab_binaries oss_clean_cython)

ADD_CUSTOM_COMMAND(
  COMMENT "make clean python directory"
  COMMAND
  ${CMAKE_COMMAND} -P
  ${CMAKE_SOURCE_DIR}/local_cmake/RemovePythonSourceTree.cmake
  ${CMAKE_CURRENT_BINARY_DIR}
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  TARGET oss_clean_python
)
