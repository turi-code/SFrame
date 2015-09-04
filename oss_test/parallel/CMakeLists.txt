project(parallel_test)


make_cxxtest(thread_tools.cxx REQUIRES parallel)
if(NOT WIN32)
make_cxxtest(lambda_omp_test.cxx REQUIRES fiber)
endif()
