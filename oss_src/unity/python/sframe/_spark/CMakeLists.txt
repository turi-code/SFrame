project(unity_spark_cython)

if(NOT WIN32) 

include_directories(${NUMPY_INCLUDE_DIRS})

function(cython_add_graphlab_module2 _module_name _sources)
  set_source_files_properties( ${_sources} PROPERTIES CYTHON_IS_CXX TRUE)
  cython_add_module( ${_module_name} ${_sources} )
  set_property(TARGET ${_module_name} APPEND_STRING PROPERTY LINK_FLAGS " -Wl,-rpath,$ORIGIN -Wl,-rpath,$ORIGIN/.. -Wl,-rpath,${CMAKE_CURRENT_BINARY_DIR} ")
  target_link_libraries(${_module_name} boost zeromq unity_core)
  add_dependencies(${_module_name} release_binaries)
endfunction()


add_custom_target(cython_isolated_targets ALL)
file(GLOB cython_sources RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} "*.pyx")
foreach(source ${cython_sources})
  set(target "")
  string(REPLACE ".pyx" "" target ${source})
  cython_add_graphlab_module2(isolated_${target} ${source})
  set_target_properties(isolated_${target} PROPERTIES OUTPUT_NAME ${target})
  add_dependencies(cython_isolated_targets isolated_${target})
endforeach()

target_link_libraries(isolated_cy_spark_unity python_callbacks spark_unity unity_core)

ADD_CUSTOM_TARGET (isolated_clean_cython)
ADD_CUSTOM_COMMAND(
  COMMENT "clean cython"
  COMMAND
  rm -f *.so *.dbg *.a *.o *.dll *.pyd *.obj
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
  TARGET isolated_clean_cython
)

make_copy_target(isolated_cython_deps ALL
  TARGETS
  ${UNITY_EXTENSION_LIBRARY}
  FILES
  ${INSTALLATION_BINARY_FILES}
  ${INSTALLATION_SYSTEM_BINARY_FILES}
  )


endif()