/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_RPC_DC_DEPENDENCY_SPLIT_HPP
#define GRAPHLAB_RPC_DC_DEPENDENCY_SPLIT_HPP

/*
 * This implements a bunch of internal functions which should really reside
 * as static functions in distributed_control. But 
 *
 */
#include <serialization/oarchive.hpp>
#include <parallel/pthread_tools.hpp>
#include <rpc/thread_local_send_buffer.hpp>

namespace graphlab {
namespace dc_impl {
extern pthread_key_t thrlocal_send_buffer_key;
extern pthread_key_t thrlocal_sequentialization_key;

/**
 * \internal
 * Obtains the thread local send buffer for a given target
 */
inline oarchive* get_thread_local_buffer(procid_t target) {
  void* ptr = pthread_getspecific(thrlocal_send_buffer_key);
  thread_local_buffer* p = (thread_local_buffer*)(ptr);
  if (p == NULL) {
    p = new thread_local_buffer;
    pthread_setspecific(thrlocal_send_buffer_key, (void*)p);
  }
  return p->acquire(target);
}

/**
 * \internal
 * Releases the thread local send buffer for the given target
 */
inline void release_thread_local_buffer(procid_t target, 
                                        bool do_not_count_bytes_sent) {
  void* ptr = pthread_getspecific(thrlocal_send_buffer_key);
  thread_local_buffer* p = (thread_local_buffer*)(ptr);
  p->release(target, do_not_count_bytes_sent);
}

/**
 * \internal
 * Writes a sequence of bytes to the local send buffer
 */
inline void write_thread_local_buffer(procid_t target, 
                                      char* c,
                                      size_t len,
                                      bool do_not_count_bytes_sent) {
  void* ptr = pthread_getspecific(thrlocal_send_buffer_key);
  thread_local_buffer* p = (thread_local_buffer*)(ptr);
  p->write(target, c, len, do_not_count_bytes_sent);
}



/**
 * \internal
 */
inline void push_flush_thread_local_buffer() {
  void* ptr = pthread_getspecific(thrlocal_send_buffer_key);
  thread_local_buffer* p = (thread_local_buffer*)(ptr);
  if (p) p->push_flush();
}

/**
 * \internal
 */
inline void pull_flush_thread_local_buffer(procid_t proc) {
  void* ptr = pthread_getspecific(thrlocal_send_buffer_key);
  thread_local_buffer* p = (thread_local_buffer*)(ptr);
  if (p) p->pull_flush(proc);
}

/**
 * \internal
 */
inline void pull_flush_soon_thread_local_buffer(procid_t proc) {
  void* ptr = pthread_getspecific(thrlocal_send_buffer_key);
  thread_local_buffer* p = (thread_local_buffer*)(ptr);
  if (p) p->pull_flush_soon(proc);
}



/**
 * \internal
 */
inline void pull_flush_soon_thread_local_buffer() {
  void* ptr = pthread_getspecific(thrlocal_send_buffer_key);
  thread_local_buffer* p = (thread_local_buffer*)(ptr);
  if (p) p->pull_flush_soon();
}



/**
 * Gets the current procid.
 * This function really exists to split the dependency between this header and
 * dc.hpp
 */
inline procid_t _get_procid() {
  void* ptr = pthread_getspecific(thrlocal_send_buffer_key);
  thread_local_buffer* p = (thread_local_buffer*)(ptr);
  if (p == NULL) {
    p = new thread_local_buffer;
    pthread_setspecific(thrlocal_send_buffer_key, (void*)p);
  }
  return p->procid;
}

/**
 * Get the current sequentialization key.
 * This function really exists to split the dependency between this header and
 * dc.hpp
 */
inline procid_t _get_sequentialization_key() {
  size_t oldval = reinterpret_cast<size_t>(pthread_getspecific(dc_impl::thrlocal_sequentialization_key));
  return (unsigned char)oldval;
}

}
}
#endif
