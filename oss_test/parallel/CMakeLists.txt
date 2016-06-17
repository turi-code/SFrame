project(parallel_test)


make_cxxtest(thread_tools.cxx REQUIRES parallel)
make_cxxtest(atomic_ops.cxx REQUIRES parallel util)
if(NOT WIN32)
make_cxxtest(lambda_omp_test.cxx REQUIRES fiber)
endif()
