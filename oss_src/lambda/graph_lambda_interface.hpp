/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_LAMBDA_GRAPH_LAMBDA_INTERFACE_HPP
#define GRAPHLAB_LAMBDA_GRAPH_LAMBDA_INTERFACE_HPP
#include <sgraph/sgraph_types.hpp>
#include <sgraph/sgraph_synchronize.hpp>
#include <cppipc/cppipc.hpp>
#include <cppipc/magic_macros.hpp>

namespace graphlab {

namespace lambda {

typedef sgraph_compute::vertex_partition_exchange vertex_partition_exchange;

GENERATE_INTERFACE_AND_PROXY(graph_lambda_evaluator_interface, graph_lambda_evaluator_proxy,
      (std::vector<sgraph_edge_data>, eval_triple_apply, (const std::vector<sgraph_edge_data>&)(size_t)(size_t)(const std::vector<size_t>&))
      (void, init, (const std::string&)(size_t)(const std::vector<std::string>&)(const std::vector<std::string>&)(size_t)(size_t))
      (void, load_vertex_partition, (size_t)(std::vector<sgraph_vertex_data>&))
      (bool, is_loaded, (size_t))
      (void, update_vertex_partition, (vertex_partition_exchange&))
      (vertex_partition_exchange, get_vertex_partition_exchange, (size_t)(const std::unordered_set<size_t>&)(const std::vector<size_t>&))
      (void, clear, )
    )
} // namespace lambda
} // namespace graphlab

#endif
