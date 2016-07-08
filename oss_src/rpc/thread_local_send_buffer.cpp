/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <rpc/thread_local_send_buffer.hpp>
#include <rpc/dc.hpp>
namespace graphlab {
namespace dc_impl {

thread_local_buffer::thread_local_buffer() {
  // allocate the buffers
  dc = distributed_control::get_instance();
  size_t nprocs = dc->numprocs(); 

  outbuf.resize(nprocs); 
  for (size_t i = 0;i < outbuf.size(); ++i) {
    outbuf[i] = new inplace_lf_queue2<buffer_elem>;
  }
  current_archive.resize(nprocs); 
  secondary_archive.buf = (char*)malloc(INITIAL_BUFFER_SIZE);
  secondary_archive.off = 0;
  secondary_archive.len = INITIAL_BUFFER_SIZE;
  archive_locks.resize(nprocs);

  bytes_sent.resize(nprocs, 0);
  dc->register_send_buffer(this);
  procid = dc->procid();
  using_secondary = false;
}


thread_local_buffer::~thread_local_buffer() {
  dc->unregister_send_buffer(this);
  push_flush();
  // deallocate the buffers
  for (size_t i = 0; i < current_archive.size(); ++i) {
    if (current_archive[i].buf) {
      free(current_archive[i].buf);
      current_archive[i].buf = NULL;
    }
  }

  for (size_t i = 0;i < outbuf.size(); ++i) {
    delete outbuf[i];
  }
  outbuf.clear();
}

void thread_local_buffer::inc_calls_sent(procid_t target) {
  dc->inc_calls_sent(target);
}


void thread_local_buffer::push_flush() {
  for (size_t i = 0; i < outbuf.size(); ++i) {
    std::pair<buffer_elem*, buffer_elem*> bufs = extract(i);
    if (bufs.first != NULL) {
      while(bufs.first != bufs.second) {
        buffer_elem* prev = bufs.first;
        dc->write_to_buffer(i, bufs.first->buf, bufs.second->len);
        buffer_elem** next = &bufs.first->next;
        volatile buffer_elem** n = (volatile buffer_elem**)(next);
        while(__unlikely__((*n) == NULL)) {
          asm volatile("pause\n": : :"memory");
        }
        bufs.first = (buffer_elem*)(*n);
        delete prev;
      }
      dc->flush_soon(i);
    }
  }
}


void thread_local_buffer::pull_flush() {
  dc->flush();
}

void thread_local_buffer::pull_flush(procid_t p) {
  dc->flush(p);
}


void thread_local_buffer::pull_flush_soon() {
  dc->flush_soon();
}


void thread_local_buffer::pull_flush_soon(procid_t p) {
  dc->flush_soon(p);
}

oarchive* thread_local_buffer::acquire(procid_t target) {
  if (archive_locks[target].try_lock()) {
    // need a new archive, or existing one at risk of being resized
    if (current_archive[target].buf == NULL) {
      current_archive[target].buf = (char*)malloc(INITIAL_BUFFER_SIZE);
      current_archive[target].off = 0;
      current_archive[target].len = INITIAL_BUFFER_SIZE;
    }
    // this is used to track the number of bytes actually written
    prev_acquire_archive_size = current_archive[target].off;
    using_secondary = false;
    return &current_archive[target];
  } else {
    // current archive is contended. Use the secondary archive
    // this is used to track the number of bytes actually written
    if (secondary_archive.buf == NULL) {
      secondary_archive.buf = (char*)malloc(INITIAL_BUFFER_SIZE);
      secondary_archive.off = 0;
      secondary_archive.len = INITIAL_BUFFER_SIZE;
    }
    prev_acquire_archive_size = secondary_archive.off;
    using_secondary = true;
    return &secondary_archive;
  }
}


void thread_local_buffer::add_to_queue(procid_t target, char* ptr, size_t len) {
  buffer_elem* elem = new buffer_elem;
  ASSERT_NE(ptr, NULL);
  elem->buf = ptr;
  elem->len = len;
  elem->next = NULL;
  outbuf[target]->enqueue(elem);
  if (outbuf[target]->approx_size() > NUM_FULL_BUFFER_LIMIT) {
    pull_flush(target);
  }
}

void thread_local_buffer::release(procid_t target, bool do_not_count_bytes_sent) {
  if (!do_not_count_bytes_sent) {
    bytes_sent[target] += current_archive[target].off - prev_acquire_archive_size - sizeof(packet_hdr);
    inc_calls_sent(target);
  }
  if (!using_secondary) {
    if (current_archive[target].off >= FULL_BUFFER_SIZE_LIMIT) {
      // shift the buffer into outbuf
      char* ptr = current_archive[target].buf;
      size_t len = current_archive[target].off;
      current_archive[target].buf = NULL; 
      current_archive[target].off = 0;
      archive_locks[target].unlock();

      add_to_queue(target, ptr, len);

    } else {
      archive_locks[target].unlock();
    }
  } else {
    // we were using the secondary archive
    // now we try to shift it to the primary
    archive_locks[target].lock();
    if (current_archive[target].off == 0) {
      // nothing in current archive, swap it in!
      std::swap(current_archive[target], secondary_archive);
      archive_locks[target].unlock();
    } else {
      // put current into the queue, put secondary into current.
      char* ptr = current_archive[target].buf;
      size_t len = current_archive[target].off;
      current_archive[target] = secondary_archive;
      archive_locks[target].unlock();
      // add both current to the queue and clear secondary
      add_to_queue(target, ptr, len);
      secondary_archive.buf = NULL;
      secondary_archive.off = 0;
    }
  }
}


void thread_local_buffer::write(procid_t target, char* c, size_t len, 
                                bool do_not_count_bytes_sent) {
  if (!do_not_count_bytes_sent) {
    bytes_sent[target] += len;
    inc_calls_sent(target);
  }
  // make sure that messsages sent before this write are sent before this write
  if (current_archive[target].off) {
    archive_locks[target].lock();

    if (current_archive[target].off) {
      add_to_queue(target, current_archive[target].buf, current_archive[target].off);
    }
    current_archive[target].buf = NULL; 
    current_archive[target].off = 0;
    archive_locks[target].unlock();
  }
  add_to_queue(target, c, len);
}


std::pair<buffer_elem*, buffer_elem*> thread_local_buffer::extract(procid_t target) {
  if (current_archive[target].off > 0 ) {
    if (archive_locks[target].try_lock()) {
      char* ptr = current_archive[target].buf;
      size_t len = current_archive[target].off;
      if (len > 0) {
        current_archive[target].buf = NULL;
        current_archive[target].off = 0;
      }
      archive_locks[target].unlock();
      if (len > 0) {
        buffer_elem* elem = new buffer_elem;
        ASSERT_NE(ptr, NULL);
        elem->buf = ptr;
        elem->len = len;
        elem->next = NULL;
        outbuf[target]->enqueue(elem);
      }
    } 
  } 
  std::pair<buffer_elem*, buffer_elem*> ret;
  ret.first = outbuf[target]->dequeue_all();
  if (ret.first != NULL) {
    ASSERT_NE(ret.first->buf, NULL);
    ret.second = outbuf[target]->end_of_dequeue_list();
    return ret;
  } else {
    return std::pair<buffer_elem*, buffer_elem*>(NULL, NULL);
  }
}



} // dc_impl
} // graphlab
