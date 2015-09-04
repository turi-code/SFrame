# Setup testing tools
# Make sure testing is enabled
enable_testing()
# Use Python interpreter
find_package(PythonInterp)
set(CXXTESTGEN ${CMAKE_SOURCE_DIR}/cxxtest/cxxtestgen)
# create a macro to define a test
macro(ADD_CXXTEST NAME)
  if(PYTHONINTERP_FOUND)
    add_custom_command(
      OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/${NAME}.cpp
      COMMAND
      ${PYTHON_EXECUTABLE} ${CXXTESTGEN}
      --runner=XUnitPrinter
      --xunit-file=TestResults-${NAME}.xml
      -o ${CMAKE_CURRENT_BINARY_DIR}/${NAME}.cpp ${ARGV}
      DEPENDS ${ARGV}
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
      )
  endif(PYTHONINTERP_FOUND)
  add_graphlab_executable(${NAME}test ${CMAKE_CURRENT_BINARY_DIR}/${NAME}.cpp)
  set_source_files_properties( ${CMAKE_CURRENT_BINARY_DIR}/${NAME}.cpp
          PROPERTIES COMPILE_FLAGS "-I${CMAKE_CURRENT_SOURCE_DIR}" GENERATED 1)

  add_test(${NAME} ${NAME}test)
endmacro(ADD_CXXTEST)

