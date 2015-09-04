/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_DC_GLOBAL_HPP
#define GRAPHLAB_DC_GLOBAL_HPP

#include<vector>
#include<cstdlib>
#include<rpc/dc_types.hpp>

namespace graphlab {

class distributed_control;
class dc_init_param;


/**
 * \ingroup rpc
 * \brief Provides global singleton mangement of dc object.
 *
 * In most cases, only a single dc objects is constructed as a stack object 
 * in the main() function.
 *
 * \code
 *  // initialize MPI
 *  mpi_tools::init(argc, argv);
 *  // construct distributed control object
 *  graphlab::distributed_control dc;
 * \endcode
 *
 * The distributed_control_global provides global access
 * to the dc objects, and behaves identially to 
 * distributed_control::get_instance().
 *
 * It is very helpful when multiple dc objects need to be allocated on demand.
 * The local state of "the current dc object in the thread" is controlled
 * by a thread_local variable, using <code>get_current_dc_idx</code> and
 * <code>set_current_dc_idx</code> which is called by 
 * individual thread who wants to create its own dc object.
 *
 * For example, the following code shows how to simulate multiple dc
 * in seperate threads:
 * \code
 *
 * void start_thread(const dc_init_param& dc_init, size_t thread_id) {
 *    distributed_control_global::set_current_dc_idx(thread_id); // thread_local
 *    auto dc = distributed_control_global::create_instance()
 *    //  do work with dc
 * }
 *
 * void launch_inproc_rpc_cloud(size_t num_nodes) {
 *   std::vector<dc_init_param> dc_params;
 *   // initialize dc_params for each node
 *
 *   std::vector<boost::thread> worker_threads;
 *   distributed_control_global::init(num_nodes);
 *   for(size_t i = 0; i < num_nodes; ++i) {
 *     worker_threads.push_back(boost::thread(start_thread));
 *   }
 *   for (auto& t: worker_threads) t.join();
 *
 *   distributed_control_global::finalize();
 * }
 * \endcode*
 */

namespace distributed_control_global {

size_t get_current_dc_idx();

void set_current_dc_idx(size_t current_thread_idx);

/**
 * Intialize the global dc singleton vector with given size.
 */
void init(size_t num_dc = 1);

/**
 * Clear all global dc states.
 * Individual dc objects must be destoryed before calling this function.
 */
void finalize();

/**
 * Create a global distributed control object
 */
distributed_control*& create_instance(const dc_init_param& init_param);

/**
 * Same as finalize, for API backwards compatibility
 */
void delete_instances();

/**
 * Return the current dc instance pointer.
 */
distributed_control*& get_instance();

/**
 * Return the current dc instance pointer.
 */
distributed_control*& get_instance(size_t dc_idx);

/**
 * Return the current dc procid pointer.
 */
procid_t& get_instance_procid();

/**
 * Set the current dc object.
 */
void set_current_dc(distributed_control* dc, procid_t procid);

} // end distributed_control_global

} // end graphlab
#endif
