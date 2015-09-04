project(rpc_test)

if(NOT WIN32)
make_executable(dc_consensus_test SOURCES dc_consensus_test.cpp REQUIRES rpc)
make_executable(dc_test_sequentialization SOURCES dc_test_sequentialization.cpp REQUIRES rpc)
make_executable(sort_test SOURCES sort_test.cpp REQUIRES rpc)
make_executable(dc_fiber_consensus_test SOURCES dc_fiber_consensus_test.cpp REQUIRES rpc)
endif()

