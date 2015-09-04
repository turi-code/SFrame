project(GraphLab)
# NOTE: do not link tcmalloc! Does not like Java.
add_jni_library(graphlabjni
  org_graphlab_Updater.cpp
  org_graphlab_Aggregator.cpp
  org_graphlab_Context.cpp
  org_graphlab_Core.cpp
  java_any.cpp)

if(JNI_REALLY_FOUND)
  # copy to ${SRC}/extapis/java_jni/lib when done
  get_target_property(graphlabjni_location graphlabjni LOCATION)
  add_custom_command(
    TARGET graphlabjni
    POST_BUILD
    COMMAND cp ${graphlabjni_location}
    ${CMAKE_SOURCE_DIR}/extapis/java_jni/lib/
    COMMENT "Copying graphlabjni to extapis/java_jni/lib")
  
  # if ant is found, build Java classes
  if(ANT_FOUND)
    add_custom_command(
      TARGET graphlabjni
      POST_BUILD
      COMMAND cd ${CMAKE_SOURCE_DIR}/extapis/java_jni && ant
      COMMENT "Building Java classes using Ant.")
  else()
    message(STATUS
      "Ant not detected. Java classes will not be built.") 
  endif()

endif()