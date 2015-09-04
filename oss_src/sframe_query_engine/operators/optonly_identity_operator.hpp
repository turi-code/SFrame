/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_SFRAME_QUERY_MANAGER_IDENTITY_NODE_HPP_
#define GRAPHLAB_SFRAME_QUERY_MANAGER_IDENTITY_NODE_HPP_

#include <flexible_type/flexible_type.hpp>
#include <random/random.hpp>
#include <parallel/pthread_tools.hpp>
#include <sframe_query_engine/operators/operator.hpp>
#include <sframe_query_engine/execution/query_context.hpp>
#include <sframe_query_engine/operators/operator_properties.hpp>

namespace graphlab {
namespace query_eval {

/**
 * A no-op operator. Does not have a physical equivalent but only has
 * a logical form. Used as a sentinel for the query optimizer.
 */
template<>
class operator_impl<planner_node_type::IDENTITY_NODE> : public query_operator {
 public:
  planner_node_type type() const { return planner_node_type::IDENTITY_NODE; }

  static std::string name() { return "identity_node";  }

  static query_operator_attributes attributes() {
    query_operator_attributes ret;
    ret.attribute_bitfield = query_operator_attributes::LOGICAL_NODE_ONLY;
    ret.num_inputs = 1;
    return ret;
  }

  ////////////////////////////////////////////////////////////////////////////////

  inline operator_impl() {}

  inline std::shared_ptr<query_operator> clone() const {
    return std::make_shared<operator_impl>(*this);
  }

  static std::shared_ptr<planner_node> make_planner_node(std::shared_ptr<planner_node> pnode) {
    auto pn = planner_node::make_shared(planner_node_type::IDENTITY_NODE);
    pn->inputs = {pnode};
    return pn;
  }

  static std::vector<flex_type_enum> infer_type(std::shared_ptr<planner_node> pnode) {
    ASSERT_EQ(pnode->inputs.size(), 1);
    return infer_planner_node_type(pnode->inputs[0]);
  }

  static int64_t infer_length(std::shared_ptr<planner_node> pnode) {
    ASSERT_EQ(pnode->inputs.size(), 1);
    return infer_planner_node_length(pnode->inputs[0]);
  }
  
};

typedef operator_impl<planner_node_type::IDENTITY_NODE> optonly_identity_operator;

} // query_eval
} // graphlab

#endif
