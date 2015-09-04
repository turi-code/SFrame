/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_SFRAME_QUERY_MANAGER_RANGE_HPP
#define GRAPHLAB_SFRAME_QUERY_MANAGER_RANGE_HPP
#include <flexible_type/flexible_type.hpp>
#include <sframe_query_engine/operators/operator.hpp>
#include <sframe_query_engine/execution/query_context.hpp>
#include <sframe_query_engine/operators/operator_properties.hpp>
namespace graphlab { 
namespace query_eval {

/**
 * A "range" operator which simply generates a range of integer values.
 */
template <>
struct operator_impl<planner_node_type::RANGE_NODE> : public query_operator {
 public:

  inline planner_node_type type() const { return planner_node_type::RANGE_NODE; }
  
  static std::string name() { return "range"; }

  static query_operator_attributes attributes() {
    query_operator_attributes ret;
    ret.attribute_bitfield = query_operator_attributes::SOURCE;
    ret.num_inputs = 0;
    return ret;
  }
  
  inline operator_impl(flex_int begin_index, flex_int end_index) { ASSERT_LE(begin_index, end_index); };
  
  inline std::string print() const {
    return name() + "(" + std::to_string(m_begin_index) + ", " + std::to_string(m_end_index) + ")";
  }

  inline std::shared_ptr<query_operator> clone() const {
    return std::make_shared<operator_impl>(*this);
  }

  inline void execute(query_context& context) {
    ASSERT_LE(m_begin_index, m_end_index);
    flex_int iter = m_begin_index;
    while(iter != m_end_index) {
      auto ret = context.get_output_buffer();
      size_t len = std::min<size_t>(m_end_index - iter, context.block_size());

      ret->resize(1, len);
      for (auto& value: *(ret->get_columns()[0])) {
        value = iter;
        ++iter;
      }
      context.emit(ret);
    }
  };

  static std::shared_ptr<planner_node> make_planner_node(
      flex_int begin_index, flex_int end_index) {
    return planner_node::make_shared(planner_node_type::RANGE_NODE, 
                                     {{"begin_index", begin_index},
                                       {"end_index", end_index}});
  }
  static std::shared_ptr<query_operator> from_planner_node(
      std::shared_ptr<planner_node> pnode) {
        ASSERT_EQ((int)pnode->operator_type, 
                  (int)planner_node_type::RANGE_NODE);
        ASSERT_TRUE(pnode->operator_parameters.count("begin_index"));
        ASSERT_TRUE(pnode->operator_parameters.count("end_index"));
        return std::make_shared<operator_impl>(pnode->operator_parameters["begin_index"],
                                               pnode->operator_parameters["end_index"]);
  }

  static std::vector<flex_type_enum> infer_type(
      std::shared_ptr<planner_node> pnode) {
    ASSERT_EQ((int)pnode->operator_type, (int)planner_node_type::RANGE_NODE);
    return {flex_type_enum::INTEGER};
  }

  static int64_t infer_length(std::shared_ptr<planner_node> pnode) {
    ASSERT_EQ((int)pnode->operator_type, (int)planner_node_type::RANGE_NODE);

    flex_int begin_index = pnode->operator_parameters.at("begin_index");
    flex_int end_index = pnode->operator_parameters.at("end_index");

    return end_index - begin_index;
  }

  static std::string repr(std::shared_ptr<planner_node> pnode, pnode_tagger&) {
    ASSERT_TRUE(pnode->operator_parameters.count("begin_index"));
    ASSERT_TRUE(pnode->operator_parameters.count("end_index"));

    size_t begin_index = pnode->operator_parameters["begin_index"];
    size_t end_index = pnode->operator_parameters["end_index"];

    std::ostringstream out;
    out << "Range(" << begin_index << ',' << end_index << ")";
    return out.str();
  }

 private:
  flex_int m_begin_index;
  flex_int m_end_index;
};

typedef operator_impl<planner_node_type::RANGE_NODE> op_range;

} // query_eval
} // graphlab

#endif // GRAPHLAB_SFRAME_QUERY_MANAGER_RANGE_HPP
