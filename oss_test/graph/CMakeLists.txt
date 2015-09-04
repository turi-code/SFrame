project(graph_test)

subdirs(data)

make_executable(distributed_graph_test SOURCES distributed_graph_test.cpp REQUIRES graph)
make_executable(distributed_graph2_test SOURCES distributed_graph2_test.cpp REQUIRES graph)
make_executable(distributed_ingress_test SOURCES distributed_ingress_test.cpp REQUIRES graph)
make_executable(test_parsers SOURCES test_parsers.cpp REQUIRES graph)
make_cxxtest(csr_storage_test.cxx REQUIRES graph)
make_cxxtest(local_graph_test.cxx REQUIRES graph)
# make_executable(dcsc_test SOURCES dcsc_test.cpp REQUIRES graph)
