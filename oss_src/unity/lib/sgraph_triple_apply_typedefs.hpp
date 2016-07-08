/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#ifndef GRAPHLAB_UNITY_SGRAPH_TRIPLE_APPLY_TYPEDEFS_HPP
#define GRAPHLAB_UNITY_SGRAPH_TRIPLE_APPLY_TYPEDEFS_HPP

#include<map>
#include<flexible_type/flexible_type.hpp>

namespace graphlab {

/**
 * Argument type and return type for sgraph triple apply.
 */
struct edge_triple {
  std::map<std::string, flexible_type> source;
  std::map<std::string, flexible_type> edge;
  std::map<std::string, flexible_type> target;
};

/**
 * Type of the triple apply lambda function.
 */
typedef std::function<void(edge_triple&)> lambda_triple_apply_fn;

}

#endif
