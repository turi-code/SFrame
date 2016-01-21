/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_LAMBDA_GRAPH_PYLAMBDA_MASTER_HPP
#define GRAPHLAB_LAMBDA_GRAPH_PYLAMBDA_MASTER_HPP

#include<lambda/graph_lambda_interface.hpp>
#include<lambda/worker_pool.hpp>

namespace graphlab {

namespace lambda {
  /**
   * \ingroup lambda
   *
   * Simple singleton object managing a worker_pool of graph lambda workers.
   */
  class graph_pylambda_master {
   public:

    static graph_pylambda_master& get_instance();

    static void shutdown_instance();

    graph_pylambda_master(size_t nworkers = 8);

    inline size_t num_workers() { return m_worker_pool->num_workers(); }

    static void set_pylambda_worker_binary(const std::string& path) { pylambda_worker_binary = path; };

    inline std::shared_ptr<worker_pool<graph_lambda_evaluator_proxy>> get_worker_pool() {
      return m_worker_pool;
    }

   private:

    graph_pylambda_master(graph_pylambda_master const&) = delete;

    graph_pylambda_master& operator=(graph_pylambda_master const&) = delete;

   private:
    std::shared_ptr<worker_pool<graph_lambda_evaluator_proxy>> m_worker_pool;

    static std::string pylambda_worker_binary;
  };
} // end lambda
} // end graphlab

#endif
