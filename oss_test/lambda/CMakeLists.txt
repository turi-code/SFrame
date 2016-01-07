project(lambda_test)

make_cxxtest(worker_pool_test.cxx REQUIRES pylambda)

make_executable(dummy_worker
  SOURCES
    dummy_worker.cpp
  REQUIRES
    cppipc
    process
)
