# for Python we prefer to dynamic link
# Assume Python is already built
add_library(python INTERFACE )
if (APPLE)
  target_link_libraries(python INTERFACE ${CMAKE_SOURCE_DIR}/deps/local/lib/${CMAKE_SHARED_LIBRARY_PREFIX}python2.7${CMAKE_SHARED_LIBRARY_SUFFIX} z)
elseif(WIN32)
  target_link_libraries(python INTERFACE ${CMAKE_SOURCE_DIR}/deps/local/lib/python27${CMAKE_SHARED_LIBRARY_SUFFIX} z)
  target_compile_definitions(python INTERFACE BOOST_PYTHON_STATIC_LIB)
  target_compile_definitions(python INTERFACE MS_WIN64)
else()
  target_link_libraries(python INTERFACE ${CMAKE_SOURCE_DIR}/deps/local/lib/${CMAKE_SHARED_LIBRARY_PREFIX}python2.7${CMAKE_SHARED_LIBRARY_SUFFIX} z rt)
endif()
target_compile_definitions(python INTERFACE HAS_PYTHON)
target_include_directories(python INTERFACE ${CMAKE_SOURCE_DIR}/deps/local/include/python2.7)
set(HAS_PYTHON TRUE CACHE BOOL "")

add_library(python_header_only INTERFACE )
if(WIN32)
  # on windows, we can't really do pure header only.
  # linking requires the dll to be around
  target_link_libraries(python_header_only INTERFACE ${CMAKE_SOURCE_DIR}/deps/local/lib/python27${CMAKE_SHARED_LIBRARY_SUFFIX} z)
  target_compile_definitions(python_header_only INTERFACE BOOST_PYTHON_STATIC_LIB)
  target_compile_definitions(python_header_only INTERFACE MS_WIN64)
endif()
target_compile_definitions(python_header_only INTERFACE HAS_PYTHON)
target_include_directories(python_header_only INTERFACE ${CMAKE_SOURCE_DIR}/deps/local/include/python2.7)
set(HAS_PYTHON_HEADER_ONLY TRUE CACHE BOOL "")
message(STATUS "HAS_PYTHON_HEADER_ONLY: ${HAS_PYTHON_HEADER_ONLY}")
