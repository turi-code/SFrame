project(serialization_test)

make_cxxtest(serializetests.cxx REQUIRES serialization)
make_cxxtest(eigen_serialization.cxx REQUIRES serialization eigen)
