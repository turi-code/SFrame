/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_SFRAME_QUERY_ENGINE_OPERATORS_QUERY_OPERATOR_HPP
#define GRAPHLAB_SFRAME_QUERY_ENGINE_OPERATORS_QUERY_OPERATOR_HPP
#include <cstddef>
#include <string>
#include <map>
#include <memory>
#include <util/any.hpp>
#include <flexible_type/flexible_type.hpp>
#include <sframe_query_engine/planning/planner_node.hpp>
#include <sframe_query_engine/operators/operator_properties.hpp>
namespace graphlab {
namespace query_eval {

class query_context;


/**
 * Basic attributes about the operator. 
 */
struct query_operator_attributes {
  enum attribute {
    NONE = 0,

    LINEAR = 1, /* A linear input operator consumes input sources at the
                         same rate and emit outputs at the same rate*/
    SUB_LINEAR = 2, /* A sublinear operator consumes input sources at the 
                       same rate, but may generate output at a different 
                       lower or higher rate */
    SOURCE = 4, /* A source operator is a direct source from an sframe
                 * or sarray and has no inputs. */

    LOGICAL_NODE_ONLY = 8, /* A node that never turns into an
                            * executor; it simply is a logical node
                            * only, possibly used in the query
                            * optimizer. */

    SUPPORTS_SKIPPING = 256, /* If the operator can correctly handle the
                                skip_next_block emit state */



  };

  size_t attribute_bitfield;  // A bitfield of the attribute enum
  int num_inputs;  // Number of inputs expected to the operator
};

/**
 *  The query operator base class.
 *
 *  All operators must inherit from this class, implementing the
 *  virtual functions described below. The member functions describe
 *  how the class behaves, which in turn describe the capabilities of
 *  the operator and how execution is performed.
 *
 *  In addition, all of the operators must implement a set of static
 *  functions that describe how they behave.   These are:
 *
 *  static std::string name()
 *
 *    Returns the name of the operator.  Used for logging.
 *
 *
 *  static std::shared_ptr<planner_node> make_planner_node(...).
 *
 *    A factory function for creating a planner node.  Takes arguments
 *    related to the operator.
 *
 *
 *  static std::shared_ptr<query_operator> from_planner_node(std::shared_ptr<planner_node> pnode)
 *
 *    Converts the planner node to its operator form.
 *
 *
 *  static std::vector<flex_type_enum> infer_type(std::shared_ptr<planner_node> pnode)
 *
 *    Returns a vector of the output types for each column.
 *
 *
 *  static int64_t infer_length(std::shared_ptr<planner_node> pnode)
 *
 *    Returns the length if known, and -1 otherwise.
 *
 */
class query_operator {
 public:

  virtual planner_node_type type() const = 0;
  
  /**
   * Basic execution attributes about the query.
   */
  inline query_operator_attributes attributes() {
    return planner_node_type_to_attributes(this->type());
  }

  /**
   * Pretty prints the operator including all additional parameters.
   */
  inline std::string name() const {
    return planner_node_type_to_name(this->type()); 
  }

  /**
   * Pretty prints the operator including all additional parameters.
   */
  virtual std::string print() const {
    return this->name();
  }
  
  /**
   * Makes a copy of the object.
   */
  virtual std::shared_ptr<query_operator> clone() const = 0;

  /**
   * Executes a query. 
   */
  virtual void execute(query_context& context) { ASSERT_TRUE(false); }

  /** The base case -- the logical-only nodes don't use this.
   *
   */
  static std::shared_ptr<query_operator> from_planner_node(std::shared_ptr<planner_node>) {
    ASSERT_TRUE(false);
    return std::shared_ptr<query_operator>();
  }

  static std::string repr(std::shared_ptr<planner_node> pnode, pnode_tagger& get_tag) {
    return planner_node_type_to_name(pnode->operator_type);
  }

};

////////////////////////////////////////////////////////////////////////////////
// The actual operator implementation

template <planner_node_type PType> 
class operator_impl : query_operator_attributes {

  planner_node_type type() const { return planner_node_type::INVALID; }
  
  std::shared_ptr<query_operator> clone() const {
    ASSERT_TRUE(false);
    return nullptr;
  }

  void execute(query_context& context) {
    ASSERT_TRUE(false);
  }
};



} // namespace query_eval
} // namespace graphlab

#endif // GRAPHLAB_SFRAME_QUERY_ENGINE_OPERATORS_QUERY_OPERATOR_HPP
