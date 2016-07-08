/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_SFRAME_QUERY_ENGINE_HPP
#define GRAPHLAB_SFRAME_QUERY_ENGINE_HPP
#include <vector>
#include <string>
#include <utility>
#include <memory>
#include <sframe/group_aggregate_value.hpp>

namespace graphlab {
class sframe;
namespace query_eval {
class planner_node;
std::shared_ptr<sframe> groupby_aggregate(
      const std::shared_ptr<planner_node>& source,
      const std::vector<std::string>& source_column_names,
      const std::vector<std::string>& keys,
      const std::vector<std::string>& output_column_names,
      const std::vector<std::pair<std::vector<std::string>,
                                  std::shared_ptr<group_aggregate_value>>>& groups);
} // namespace query_eval
} // namespace graphlab
#endif
