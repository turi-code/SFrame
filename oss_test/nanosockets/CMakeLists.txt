project(nanosockets)

make_cxxtest(reqreptest.cxx REQUIRES nanosockets)
make_cxxtest(pubsubtest.cxx REQUIRES nanosockets)
