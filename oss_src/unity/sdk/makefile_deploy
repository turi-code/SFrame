.PHONY: doc clean all

CXX := g++
CXXFLAGS := -std=c++11 -I . -shared -fPIC

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
	CXXFLAGS += --stdlib=libc++ -undefined dynamic_lookup
endif

#### SDK Examples ##### 
EXAMPLE_SRCS := $(wildcard sdk_example/*.cpp)
EXAMPLE_TARGETS := $(EXAMPLE_SRCS:%.cpp=%.so)

sdk_example : $(EXAMPLE_TARGETS)

sdk_example/%.so: sdk_example/%.cpp
	$(CXX) -o $@ $(CXXFLAGS) $^

#### Doxygen Documentation #####
doc:
	cd doxygen && doxygen

#### Clean Target ####
clean:
	rm sdk_example/*.so


#### All targets ####
all: sdk_example 
