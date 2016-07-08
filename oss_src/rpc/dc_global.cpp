/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include<rpc/dc_global.hpp>
#include<rpc/dc.hpp>
#include<parallel/mutex.hpp>
#include <util/any.hpp>

namespace graphlab {

namespace  distributed_control_global {

bool global_dc_inited = false;
// The integer key for thread local dc index in the tls_data. 
const size_t DC_INDEX_KEY_IN_TLS = 0;

// global singleton vector to store dc objects
static std::vector<distributed_control*> global_dc_vec;
// global singleton vector to store dc procid
static std::vector<procid_t> global_dc_procid_vec;

void init(size_t num_dc) {
  static mutex mtx;
  mtx.lock();
  ASSERT_FALSE(global_dc_inited);
  global_dc_vec.resize(num_dc, NULL);
  global_dc_procid_vec.resize(num_dc, 0);
  global_dc_inited = true;
  mtx.unlock();
}

void finalize() {
  static mutex mtx;
  mtx.lock();
  for (auto& dc : global_dc_vec) { 
    if (dc != NULL) delete dc;
  }
  global_dc_vec.clear();
  global_dc_procid_vec.clear();
  global_dc_inited = false;
  mtx.unlock();
}

void set_current_dc_idx(size_t current_thread_idx) {
  auto& tls = thread::get_tls_data();
  tls[DC_INDEX_KEY_IN_TLS] = (any)current_thread_idx;
}

size_t get_current_dc_idx() {
  auto& tls = thread::get_tls_data();
  if (tls.contains(DC_INDEX_KEY_IN_TLS)) {
    return tls[DC_INDEX_KEY_IN_TLS].as<size_t>();
  }
  return 0;
}

distributed_control*& create_instance(const dc_init_param& init_param) {
  // the dc constructor takes care of putting the pointer
  // into the global dc vector using set_current_dc
  new distributed_control(init_param);
  return get_instance();
}

void delete_instances() {
  finalize();
}

distributed_control*& get_instance() {
  return get_instance(get_current_dc_idx());
}

distributed_control*& get_instance(size_t dc_idx) {
  if (!global_dc_inited) init();
  ASSERT_MSG(dc_idx < global_dc_vec.size(),
      "Current dc index out of bound. Forgot to call distributed_control_global::init(num_nodes)?");
  auto& ret = global_dc_vec[dc_idx];
  return ret;
}

procid_t& get_instance_procid() {
  if (!global_dc_inited) init();
  size_t current_dc_idx = get_current_dc_idx();
  ASSERT_MSG(current_dc_idx < global_dc_vec.size(),
      "Current dc index out of bound. Forgot to call distributed_control_global::init(num_nodes)?");
  return global_dc_procid_vec[current_dc_idx];
}

/**
 * Private, internally used by dc construction only.
 */
void set_current_dc(distributed_control* dc, procid_t procid) {
  size_t current_dc_idx = get_current_dc_idx();

  // make sure no overwrite
  ASSERT_LT(current_dc_idx, global_dc_vec.size());
  ASSERT_EQ(global_dc_vec[current_dc_idx], NULL);

  global_dc_vec[current_dc_idx] = dc;
  global_dc_procid_vec[current_dc_idx] = procid;
}

} // end distributed_control_global

} // end graphlab
