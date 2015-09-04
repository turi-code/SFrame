project(sframe_query_engine_test)

make_cxxtest(basic_end_to_end.cxx REQUIRES sframe sframe_query_engine)
make_cxxtest(optimizations.cxx REQUIRES sframe sframe_query_engine)

subdirs(operators)
