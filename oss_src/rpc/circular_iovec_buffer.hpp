/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
/*
 * Copyright (c) 2009 Carnegie Mellon University.
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS
 *  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied.  See the License for the specific language
 *  governing permissions and limitations under the License.
 *
 * For more about this software visit:
 *
 *      http://www.graphlab.ml.cmu.edu
 *
 */


#ifndef GRAPHLAB_RPC_CIRCULAR_IOVEC_BUFFER_HPP
#define GRAPHLAB_RPC_CIRCULAR_IOVEC_BUFFER_HPP
#include <vector>
#include <sys/socket.h>

namespace graphlab{
namespace dc_impl {

/**
 * \ingroup rpc
 * \internal
 * A circular buffer which maintains a parallel sequence of iovecs.
 * One sequence is basic iovecs
 * The other sequence is used for storing the original unomidifed pointers
 * This is minimally checked. length must be a power of 2
 */
struct circular_iovec_buffer {
  inline circular_iovec_buffer(size_t len = 4096) {
    v.resize(4096);
    parallel_v.resize(4096);
    head = 0;
    tail = 0;
    numel = 0;
  }

  inline bool empty() const {
    return numel == 0;
  }

  size_t size() const {
    return numel;
  }


  void reserve(size_t _n) {
    if (_n <= v.size()) return;
    size_t originalsize = v.size();
    size_t n = v.size();
    // must be a power of 2
    while (n <= _n) n *= 2;

    v.resize(n);
    parallel_v.resize(n);
    if (head >= tail && numel > 0) {
      // there is a loop around
      // we need to fix the shift
      size_t newtail = originalsize;
      for (size_t i = 0;i < tail; ++i) {
        v[newtail] = v[i];
        parallel_v[newtail] = parallel_v[i];
        ++newtail;
      }
      tail = newtail;
    }
  }

  inline void write(const std::vector<iovec>& other, size_t nwrite) {
    reserve(numel + nwrite);
    for (size_t i = 0;i < nwrite; ++i) {
      v[tail] = other[i];
      parallel_v[tail] = other[i];
      tail = (tail + 1) & (v.size() - 1);
    }
    numel += nwrite;

  }

  /**
   * Writes an entry into the buffer, resizing the buffer if necessary.
   * This buffer will take over all iovec pointers and free them when done
   */
  inline void write(const iovec &entry) {
    if (numel == v.size()) {
      reserve(2 * numel);
    }

    v[tail] = entry;
    parallel_v[tail] = entry;
    tail = (tail + 1) & (v.size() - 1); ++numel;
  }


  /**
   * Writes an entry into the buffer, resizing the buffer if necessary.
   * This buffer will take over all iovec pointers and free them when done.
   * This version of write allows the iovec that is sent to be different from the
   * iovec that is freed. (for instance, what is sent could be subarray of
   * what is to be freed.
   */
  inline void write(const iovec &entry, const iovec& actual_ptr_entry) {
    if (numel == v.size()) {
      reserve(2 * numel);
    }

    v[tail] = actual_ptr_entry;
    parallel_v[tail] = entry;
    tail = (tail + 1) & (v.size() - 1); ++numel;
  }


  /**
   * Erases a single iovec from the head and free the pointer
   */
  inline void erase_from_head_and_free() {
    free(v[head].iov_base);
    head = (head + 1) & (v.size() - 1);
    --numel;
  }

  /**
   * Fills a msghdr for unsent data.
   */
  void fill_msghdr(struct msghdr& data) {
    data.msg_iov = &(parallel_v[head]);
    if (head < tail) {
      data.msg_iovlen = tail - head;
    }
    else {
      data.msg_iovlen = v.size() - head;
    }
    data.msg_iovlen = std::min<size_t>(IOV_MAX, data.msg_iovlen);
  }

  /**
   * Advances the head as if some amount of data was sent.
   */
  void sent(size_t len) {
    while(len > 0) {
      size_t curv_sent_len = std::min(len, parallel_v[head].iov_len);
      parallel_v[head].iov_len -= curv_sent_len;
      parallel_v[head].iov_base = (char*)(parallel_v[head].iov_base) + curv_sent_len;
      len -= curv_sent_len;
      if (parallel_v[head].iov_len == 0) {
        erase_from_head_and_free();
      }
    }
  }

  std::vector<struct iovec> v;
  std::vector<struct iovec> parallel_v;
  size_t head;
  size_t tail;
  size_t numel;
};

}
}

#endif
