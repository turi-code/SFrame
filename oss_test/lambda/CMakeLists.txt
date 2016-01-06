project(lambda_test)

make_cxxtest(worker_pool_test.cxx REQUIRES pylambda)

make_executable(dummy_worker
  SOURCES
    dummy_worker.cpp
  REQUIRES
    cppipc
    process
)

make_library(pyflexible_type_bench
  SOURCES
    pyflexible_type_bench.cpp SHARED EXTERNAL_VISIBILITY
  REQUIRES 
    pylambda pylambda_worker_lib
)
