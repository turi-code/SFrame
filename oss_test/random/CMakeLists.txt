project(random_test)
#make_cxxtest(random_test.cxx REQUIRES random)
make_executable(alias_benchmark SOURCES test_alias.cpp REQUIRES random)
