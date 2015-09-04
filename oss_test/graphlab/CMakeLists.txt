project(graphlab_test)
subdirs(data)

#ADD_CXXTEST(factor_test.cxx)
make_cxxtest(small_map_test.cxx REQUIRES logger)
make_cxxtest(small_set_test.cxx REQUIRES logger)
make_cxxtest(dense_bitset_test.cxx REQUIRES logger)

make_cxxtest(test_lock_free_pool.cxx REQUIRES logger)
make_cxxtest(lock_free_pushback.cxx REQUIRES logger)
make_cxxtest(union_find_test.cxx REQUIRES logger random)

make_cxxtest(empty_test.cxx)
# ADD_CXXTEST(scheduler_test.cxx)

make_executable(cuckootest SOURCES cuckootest.cpp REQUIRES random perf)
make_executable(hopscotch_test SOURCES hopscotch_test.cpp REQUIRES random perf)

# make_executable(distributed_chandy_misra_test SOURCES distributed_chandy_misra_test.cpp REQUIRES graphlab)
# make_executable(synchronous_engine_test SOURCES synchronous_engine_test.cpp REQUIRES graphlab)
# add_test(synchronous_engine_test synchronous_engine_test)
# make_executable(async_consistent_test SOURCES async_consistent_test.cpp REQUIRES graphlab)
# add_test(async_consistent_test async_consistent_test)

if(NOT WIN32)
make_executable(sfinae_function_test SOURCES sfinae_function_test.cpp REQUIRES graphlab)
endif()

# make_executable(test_vertex_set SOURCES test_vertex_set.cpp REQUIRES graphlab)
# add_test(test_vertex_set test_vertex_set)

# make_executable(arbitrary_signal_test SOURCES arbitrary_signal_test.cpp REQUIRES graphlab)

# Move these to dht
# make_cxxtest(vector_dht_test.cxx REQUIRES graphlab)
# make_cxxtest(scalar_dht_vect_test.cxx REQUIRES graphlab)
make_cxxtest(bitops_intrinsics.cxx REQUIRES logger)
make_cxxtest(symmetric_array.cxx REQUIRES logger)
make_cxxtest(logit_math_test.cxx REQUIRES logger)
