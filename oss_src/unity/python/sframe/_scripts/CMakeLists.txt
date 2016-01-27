project(unity_cython_scripts)

set_source_files_properties("pyspark_unity.pyx" PROPERTIES CYTHON_IS_CXX TRUE)
set_source_files_properties("../cython/cy_flexible_type.pyx" PROPERTIES CYTHON_IS_CXX TRUE)
set_source_files_properties("../cython/cy_callback.pyx" PROPERTIES CYTHON_IS_CXX TRUE)

# This is annoying
get_directory_property( cur_include_directories INCLUDE_DIRECTORIES )
list( APPEND cur_include_directories "${CMAKE_CURRENT_SOURCE_DIR}/../cython")
set_directory_properties(PROPERTIES INCLUDE_DIRECTORIES "${cur_include_directories}")

# To avoid complicated library dependencies on these modules, we need
# to include additional cython files as part of this executable.

cython_add_standalone_executable(pyspark_unity MAIN_MODULE
   "pyspark_unity.pyx" "../cython/cy_flexible_type.pyx"
   "../cython/cy_callback.pyx")

set_property(TARGET pyspark_unity
             APPEND_STRING PROPERTY
             LINK_FLAGS " -Wl,-rpath,$ORIGIN -Wl,-rpath,$ORIGIN/.. -Wl,-rpath,${CMAKE_CURRENT_BINARY_DIR} ")

target_link_libraries(pyspark_unity python_callbacks spark_unity unity_core)
add_dependencies(pyspark_unity release_binaries)

