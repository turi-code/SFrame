project(fiber)

make_library(fiber
  SOURCES
    fiber_control.cpp
    fiber_group.cpp
    fiber_async_consensus.cpp
  REQUIRES
    parallel logger random
    EXTERNAL_VISIBILITY
)
