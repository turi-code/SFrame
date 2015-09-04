project(sgraph_test)

make_cxxtest(hilbert_par_for.cxx REQUIRES sgraph)
make_cxxtest(sgraph_test.cxx REQUIRES sgraph)
make_cxxtest(sgraph_vertex_apply_test.cxx REQUIRES sgraph)
make_cxxtest(sgraph_engine_test.cxx REQUIRES sgraph)
make_cxxtest(sgraph_triple_apply_test.cxx REQUIRES sgraph)
make_cxxtest(sgraph_fast_triple_apply_test.cxx REQUIRES sgraph)
make_executable(sgraph_bench SOURCES sgraph_bench.cpp REQUIRES sgraph)
