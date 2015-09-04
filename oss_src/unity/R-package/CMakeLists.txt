project(unity)

set(OSS_R_SDK ${CMAKE_SOURCE_DIR}/oss_src/unity/sdk/graphlab)

set(RCPP_WORKER ${CMAKE_CURRENT_BINARY_DIR}/../../../oss_src/lambda/rcpplambda_worker)

set(RCPP_LAMBDA_LIB ${CMAKE_CURRENT_BINARY_DIR}/../../../oss_src/lambda/librcpplambda_shared.*)

set(R_PKG_LIB ${CMAKE_CURRENT_BINARY_DIR}/../../../oss_src/unity/libunity_shared.*)

set(R_HOME ${CMAKE_CURRENT_BINARY_DIR}/../../../../deps/R)

ADD_CUSTOM_TARGET (R_source ALL)
ADD_CUSTOM_COMMAND(
  COMMENT "build prop R source"
  COMMAND
  ${CMAKE_COMMAND} -E
  copy_directory ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_BINARY_DIR}
  COMMAND cp ${R_PKG_LIB} ${CMAKE_CURRENT_BINARY_DIR}/src/libunity_shared.so
  COMMAND cp ${RCPP_LAMBDA_LIB} ${CMAKE_CURRENT_BINARY_DIR}/src/librcpplambda_shared.so
  COMMAND cp ${CMAKE_SOURCE_DIR}/oss_src/lambda/rcpplambda_utils.hpp ${CMAKE_CURRENT_BINARY_DIR}/inst/include/graphlab/
  COMMAND cp ${RCPP_WORKER} ${CMAKE_CURRENT_BINARY_DIR}/inst/
  COMMAND find ./inst -name "*.txt" -type f|xargs rm -f
  TARGET R_source
)

add_dependencies(R_source rcpplambda_shared rcpplambda_worker unity_shared)
