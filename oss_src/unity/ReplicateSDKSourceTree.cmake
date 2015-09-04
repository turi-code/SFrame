# Note: when executed in the build dir, then CMAKE_CURRENT_SOURCE_DIR is the
# build dir.

file(GLOB_RECURSE sdkfiles FOLLOW_SYMLINKS RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}" "sdk/graphlab/*")

## Copy sdk source files
foreach(item ${sdkfiles})

  if(${CMAKE_VERSION} VERSION_GREATER 2.8.11.2)
    get_filename_component(dirname ${item} DIRECTORY)
  else()
    get_filename_component(dirname ${item} PATH)
  endif()

  get_filename_component(fname ${item} NAME)
  if ("${fname}" MATCHES ".*\\.h$" OR 
      "${fname}" MATCHES ".*\\.hpp$" OR 
      "${fname}" MATCHES ".*\\.dox$" OR 
      "${fname}" MATCHES "^LICENSE$" OR
      "${fname}" MATCHES "^Makefile$")
    #message(STATUS "Copy ${item} to ${CMAKE_ARGV3}/${dirname}")
    get_filename_component(fname ${item} REALPATH)
    file(COPY ${fname} DESTINATION "${CMAKE_ARGV3}/${dirname}")
    
  endif()
endforeach()

## Copy sdk examples, and patch the source code include path
file(COPY sdk/sdk_example DESTINATION "${CMAKE_ARGV3}/sdk" )
execute_process(COMMAND find ./graphlab \( -name "*.cpp" -or -name "*.hpp" \) -exec ${CMAKE_ARGV3}/../../../oss_local_scripts/patch_sdk.py {} \; WORKING_DIRECTORY ${CMAKE_ARGV3}/sdk)
execute_process(COMMAND find ./sdk_example \( -name "*.cpp" -or -name "*.hpp" \) -exec ${CMAKE_ARGV3}/../../../oss_local_scripts/patch_sdk.py {} \; WORKING_DIRECTORY ${CMAKE_ARGV3}/sdk)

## Copy makefile and license
file(COPY sdk/LICENSE DESTINATION "${CMAKE_ARGV3}/sdk" )
file(COPY sdk/makefile_deploy DESTINATION "${CMAKE_ARGV3}/sdk" )
file(COPY sdk/makefile_template DESTINATION "${CMAKE_ARGV3}/sdk" )
file(RENAME "${CMAKE_ARGV3}/sdk/makefile_deploy" "${CMAKE_ARGV3}/sdk/Makefile")

## Copy boost
set (boostfiles "")
# find all direct included boost headers
execute_process(COMMAND ${CMAKE_ARGV3}/../../../oss_local_scripts/find_boost_include.sh ${sdkfiles} OUTPUT_VARIABLE boostfiles)
string(REPLACE "\n" ";" boostfiles ${boostfiles})
# use bcp to recursively copy all dependent headers
set (bcp ${CMAKE_ARGV3}/../../../deps/local/bin/bcp)
execute_process(COMMAND ${bcp} --boost=${CMAKE_ARGV3}/../../../deps/local/include ${boostfiles} ${CMAKE_ARGV3}/sdk)

## Copy doxygen files
file(COPY sdk/doxygen DESTINATION "${CMAKE_ARGV3}/sdk")
