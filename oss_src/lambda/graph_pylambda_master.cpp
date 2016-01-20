/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <lambda/graph_pylambda_master.hpp>
#include <parallel/lambda_omp.hpp>
#include <lambda/lambda_constants.hpp>
#include <lambda/lambda_master.hpp>

namespace graphlab {

namespace lambda {

static graph_pylambda_master* instance_ptr = nullptr;

graph_pylambda_master& graph_pylambda_master::get_instance() {
  if (instance_ptr == nullptr) {
    size_t num_workers = (std::min<size_t>(DEFAULT_NUM_GRAPH_LAMBDA_WORKERS, std::max<size_t>(thread::cpu_count(), 1)));
    instance_ptr = new graph_pylambda_master(num_workers);
  }
  return *instance_ptr;
}

void graph_pylambda_master::shutdown_instance() {
  if (instance_ptr) {
    logstream(LOG_INFO) << "Shutdown graph lambda workers" << std::endl;
    delete instance_ptr;
    instance_ptr = nullptr;
  }
} 

graph_pylambda_master::graph_pylambda_master(size_t nworkers) {
  std::vector<std::string> worker_addresses;
  for (size_t i = 0; i < nworkers; ++i) {
    worker_addresses.push_back(std::string("ipc://") + get_temp_name());
  }
  m_worker_pool.reset(
      new worker_pool<graph_lambda_evaluator_proxy>(
          nworkers,
          lambda_master::get_lambda_worker_binary(),
          worker_addresses));

  if (nworkers < thread::cpu_count()) {
    logprogress_stream << "Using default " << nworkers << " lambda workers.\n";
    logprogress_stream << "To maximize the degree of parallelism, add the following code to the beginning of the program:\n";
    logprogress_stream << "\"graphlab.set_runtime_config(\'GRAPHLAB_DEFAULT_NUM_GRAPH_LAMBDA_WORKERS\', " << thread::cpu_count() << ")\"\n";
    logprogress_stream << "Note that increasing the degree of parallelism also increases the memory footprint." << std::endl;
  }
}

} // end of lambda
} // end of graphlab
