/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_RPC_THREAD_LOCAL_SEND_BUFFER_HPP
#define GRAPHLAB_RPC_THREAD_LOCAL_SEND_BUFFER_HPP
#include <serialization/oarchive.hpp>
#include <rpc/dc_compile_parameters.hpp>
#include <rpc/dc_internal_types.hpp>
#include <util/dense_bitset.hpp>
#include <graphlab/util/inplace_lf_queue2.hpp>
namespace graphlab {
class distributed_control;


namespace dc_impl {

struct thread_local_buffer {
  std::vector<inplace_lf_queue2<buffer_elem>* > outbuf;
  std::vector<size_t> bytes_sent;


  std::vector<mutex> archive_locks;
  // where all the writes happen
  std::vector<oarchive> current_archive; 
  // where all the contended writes happen
  oarchive secondary_archive;
  size_t prev_acquire_archive_size;
  bool using_secondary;

  procid_t procid;
  distributed_control* dc;

  thread_local_buffer();
  ~thread_local_buffer();

  /**
   * Must be called from within the thread owning this buffer.
   * Acquires a buffer to write to
   */
  oarchive* acquire(procid_t target);

  inline size_t get_bytes_sent(procid_t target) {
    return bytes_sent[target];
  }
  /**
   * Must be called from within the thread owning this buffer.
   * Releases a buffer previously acquired with acquire
   */
  void release(procid_t target, bool do_not_count_bytes_sent);

  void write(procid_t target, char* c, size_t len, bool do_not_count_bytes_sent);

  /**
   * Must be called from within the thread owning this buffer.
   * Flushes the buffer to the sender. This should really only be used
   * when the thread is dying since this incurs a large performance penalty by
   * locking up the sender.
   */
  void push_flush();


  /**
   * Can be called anywhere.
   * Flushes the buffer to the sender. This function blocks until all 
   * buffers have been flushed. Equivalent to calling distributed_control::flush()
   */
  void pull_flush();

  /**
   * Can be called anywhere.
   * Flushes the buffer to the sender. This function blocks until all 
   * buffers have been flushed. Equivalent to calling distributed_control::flush()
   */
  void pull_flush(procid_t p);

  /**
   * Can be called anywhere.
   * Flushes the buffer to the sender. This function requests a flush to happen
   * soon. Equivalent to calling distributed_control::flush()
   */
  void pull_flush_soon();


  /**
   * Can be called anywhere.
   * Flushes the buffer to the sender. This function requests a flush to happen
   * soon. Equivalent to calling distributed_control::flush()
   */
  void pull_flush_soon(procid_t p);

  /**
   * Extracts the buffer going to a given target.
   * The first element of the pair points to the head of the linked list
   * The linked list ends when the pointer becomes the second element of 
   * the pair.
   */
  std::pair<buffer_elem*, buffer_elem*> extract(procid_t target);

  void inc_calls_sent(procid_t target);

  void add_to_queue(procid_t target, char* ptr, size_t len);
};
}
}
#endif
