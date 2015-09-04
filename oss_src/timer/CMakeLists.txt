project(random)

#/**************************************************************************/
#/*                                                                        */
#/*                             Random Library                             */
#/*                                                                        */
#/**************************************************************************/

make_library(timer
  SOURCES
    timer.cpp
  REQUIRES
    parallel
    EXTERNAL_VISIBILITY
  )
