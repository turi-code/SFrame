project(nanosockets)

make_cxxtest(reqreptest.cxx REQUIRES nanosockets tcmalloc)
make_cxxtest(pubsubtest.cxx REQUIRES nanosockets tcmalloc)
