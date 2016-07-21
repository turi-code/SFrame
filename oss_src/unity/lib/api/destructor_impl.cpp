#include "unity_global_interface.hpp"
#include "unity_graph_interface.hpp"
#include "unity_sarray_builder_interface.hpp"
#include "unity_sarray_interface.hpp"
#include "unity_sframe_builder_interface.hpp"
#include "unity_sframe_interface.hpp"
#include "unity_sketch_interface.hpp"

namespace graphlab {
GENERATE_BASE_DESTRUCTOR(unity_global_base)
GENERATE_BASE_DESTRUCTOR(unity_sgraph_base)
GENERATE_BASE_DESTRUCTOR(unity_sarray_builder_base)
GENERATE_BASE_DESTRUCTOR(unity_sarray_base)
GENERATE_BASE_DESTRUCTOR(unity_sframe_builder_base)
GENERATE_BASE_DESTRUCTOR(unity_sframe_base)
GENERATE_BASE_DESTRUCTOR(unity_sketch_base)
}
