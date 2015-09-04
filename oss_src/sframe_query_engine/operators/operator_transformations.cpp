/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <sframe_query_engine/operators/operator_transformations.hpp>
#include <sframe_query_engine/operators/all_operators.hpp>
#include <sframe_query_engine/planning/planner_node.hpp>
#include <logger/assertions.hpp>

namespace graphlab { namespace query_eval {

/** Turns a node graph into one with all the source nodes segmented.
 *  Used to run a section in parallel.
 */
pnode_ptr make_segmented_graph(pnode_ptr n, size_t segment_idx, 
    size_t num_segments, std::map<pnode_ptr, pnode_ptr>& memo) {
  if (memo.count(n)) return memo[n];
  if(num_segments == 0) {
    memo[n] = n;
    return n;
  }

  pnode_ptr ret(new planner_node(*n));

  if(is_source_node(n)) {

    // First, if it's a source node, then it should have begin_index,
    // and end_index in the operarator_parameters.

    DASSERT_TRUE(n->operator_parameters.count("begin_index"));
    DASSERT_TRUE(n->operator_parameters.count("end_index"));

    size_t old_begin_index = n->operator_parameters.at("begin_index");
    size_t old_end_index = n->operator_parameters.at("end_index");

    size_t old_length = old_end_index - old_begin_index;

    size_t new_begin_index = old_begin_index + (segment_idx * old_length) / num_segments;
    size_t new_end_index = old_begin_index + ((segment_idx + 1) * old_length) / num_segments;

    DASSERT_LE(old_begin_index, new_begin_index);
    DASSERT_LE(new_end_index, old_end_index);

    ret->operator_parameters["begin_index"] = new_begin_index;
    ret->operator_parameters["end_index"] = new_end_index;

  } else {
    for(size_t i = 0; i < ret->inputs.size(); ++i) {
      ret->inputs[i] = make_segmented_graph(ret->inputs[i], segment_idx, num_segments, memo);
    }
  }
  memo[n] = ret;
  return ret;
}

}}
