/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_UNITY_SKETCH_INTERFACE_HPP
#define GRAPHLAB_UNITY_SKETCH_INTERFACE_HPP
#include <memory>
#include <vector>
#include <map>
#include <string>

#include <flexible_type/flexible_type.hpp>
#include <unity/lib/api/unity_sarray_interface.hpp>
#include <cppipc/magic_macros.hpp>

namespace graphlab {
class unity_sketch_base;

typedef std::pair<flexible_type, size_t> item_count;
typedef std::map<flexible_type, std::shared_ptr<unity_sketch_base>> sub_sketch_map;

GENERATE_INTERFACE_AND_PROXY_NO_INLINE_DESTRUCTOR(unity_sketch_base, unity_sketch_proxy,
    (void, construct_from_sarray, (std::shared_ptr<unity_sarray_base>)(bool)(const std::vector<flexible_type>&))
    (double, get_quantile, (double))
    (double, frequency_count, (flexible_type))
    (std::vector<item_count>, frequent_items, )
    (double, num_unique, )
    (double, mean, )
    (double, max, )
    (double, min, )
    (double, var, )
    (size_t, size, )
    (double, sum, )
    (size_t, num_undefined, )
    (bool, sketch_ready, )
    (size_t, num_elements_processed, )
    (std::shared_ptr<unity_sketch_base>, element_summary, )
    (std::shared_ptr<unity_sketch_base>, element_length_summary, )
    (std::shared_ptr<unity_sketch_base>, dict_key_summary, )
    (std::shared_ptr<unity_sketch_base>, dict_value_summary, )
    (sub_sketch_map, element_sub_sketch, (const std::vector<flexible_type>&))
    (void, cancel, )
)

} // namespace graphlab
#endif // GRAPHLAB_UNITY_GRAPH_INTERFACE_HPP
