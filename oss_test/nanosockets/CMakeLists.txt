project(nanosockets)

make_cxxtest(reqreptest.cxx REQUIRES nanosockets tcmalloc)
