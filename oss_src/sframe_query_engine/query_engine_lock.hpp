/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_SFRAME_QUERY_ENGINE_HPP
#define GRAPHLAB_SFRAME_QUERY_ENGINE_HPP
#include <thread>
namespace graphlab {
class recursive_mutex;
namespace query_eval {

/**
 * A global lock around all external entry points to the query execution.
 * For now, 
 * - materialize()
 * - infer_planner_node_type()
 * - infer_planner_node_length()
 */
extern recursive_mutex global_query_lock;
} // query_eval
} // graphlab
#endif
