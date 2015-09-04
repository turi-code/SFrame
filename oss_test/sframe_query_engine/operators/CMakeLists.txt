project(sframe_query_engine_test)

make_cxxtest(sarray_source.cxx REQUIRES sframe sframe_query_engine)
make_cxxtest(sframe_source.cxx REQUIRES sframe sframe_query_engine)
make_cxxtest(project.cxx REQUIRES sframe sframe_query_engine)
make_cxxtest(transform.cxx REQUIRES sframe sframe_query_engine)
make_cxxtest(append.cxx REQUIRES sframe sframe_query_engine)
make_cxxtest(binary_transform.cxx REQUIRES sframe sframe_query_engine)
make_cxxtest(logical_filter.cxx REQUIRES sframe sframe_query_engine)
make_cxxtest(union.cxx REQUIRES sframe sframe_query_engine)

# The lambda test requires a pickled function without graphlab dependency
# make_cxxtest(lambda_transform.cxx REQUIRES sframe sframe_query_engine)
