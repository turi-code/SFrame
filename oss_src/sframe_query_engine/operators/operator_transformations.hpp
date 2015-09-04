/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_SFRAME_QUERY_OPERATOR_TRANSFORMATIONS_H_
#define GRAPHLAB_SFRAME_QUERY_OPERATOR_TRANSFORMATIONS_H_

#include <sframe_query_engine/operators/all_operators.hpp>
#include <sframe_query_engine/operators/operator_properties.hpp>

namespace graphlab { namespace query_eval {

/** Turns a node graph into the proper segmented part.
 */
pnode_ptr make_segmented_graph(pnode_ptr n, size_t split_idx, size_t n_splits,
    std::map<pnode_ptr, pnode_ptr>& memo);

}}

#endif /* _OPERATOR_TRANSFORMATIONS_H_ */
