/**
 * Copyright (C) 2015 Dato, Inc.
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


#ifndef GRAPHLAB_RPC_SAMPLE_SORT_HPP
#define GRAPHLAB_RPC_SAMPLE_SORT_HPP

#include <vector>
#include <algorithm>
#include <utility>
#include <rpc/dc_dist_object.hpp>
#include <rpc/buffered_exchange.hpp>
#include <logger/assertions.hpp>
namespace graphlab {

namespace sample_sort_impl {
  template <typename Key, typename Value>
  struct pair_key_comparator {
    bool operator()(const std::pair<Key,Value>& k1,
                    const std::pair<Key,Value>& k2) {
      return k1.first < k2.first;
    }
  };
}

template <typename Key, typename Value>
class sample_sort {
 private:
  dc_dist_object<sample_sort<Key, Value> > rmi;

  typedef buffered_exchange<std::pair<Key, Value> > key_exchange_type;

  key_exchange_type key_exchange;
  std::vector<std::pair<Key, Value> > key_values;
 public:
  sample_sort(distributed_control& dc): rmi(dc, this), key_exchange(dc) { }

  template <typename KeyIterator, typename ValueIterator>
  void sort(KeyIterator kstart, KeyIterator kend,
            ValueIterator vstart, ValueIterator vend) {
    rmi.barrier();

    size_t num_entries = std::distance(kstart, kend);
    ASSERT_EQ(num_entries, std::distance(vstart, vend));

    // we will sample k * p entries
    std::vector<std::vector<Key> > sampled_keys(rmi.numprocs());
    for (size_t i = 0;i < 100 * rmi.numprocs(); ++i) {
      size_t idx = (rand() % num_entries); 
      sampled_keys[rmi.procid()].push_back(*(kstart + idx));
    }

    rmi.all_gather(sampled_keys);
    // collapse into a single array and sort
    std::vector<Key> all_sampled_keys;
    for (size_t i = 0;i < sampled_keys.size(); ++i) {
      std::copy(sampled_keys[i].begin(), sampled_keys[i].end(),
                std::inserter(all_sampled_keys, all_sampled_keys.end()));
    }
    // sort the sampled keys and extract the ranges
    std::sort(all_sampled_keys.begin(), all_sampled_keys.end());
    std::vector<Key> ranges(rmi.numprocs());
    ranges[0] = Key();
    for(size_t i = 1; i < rmi.numprocs(); ++i) {
      ranges[i] = all_sampled_keys[sampled_keys[0].size() * i];
    }

    // begin shuffle 
    KeyIterator kiter = kstart;
    ValueIterator viter = vstart;
    if (rmi.numprocs() < 8) {
      while(kiter != kend) {
        procid_t target_machine = 0;
        while (target_machine < rmi.numprocs() - 1  && 
               ranges[target_machine + 1] < *kiter) ++target_machine;
        key_exchange.send(target_machine, std::make_pair(*kiter, *viter));
        ++kiter; ++viter;
      }
    } 
    else {
      while(kiter != kend) {
        procid_t target_machine = 
          std::upper_bound(ranges.begin(), ranges.end(), *kiter) 
          - ranges.begin() - 1;
        key_exchange.send(target_machine, std::make_pair(*kiter, *viter));
        ++kiter; ++viter;
      }
    }
    key_exchange.flush();
 
    // read from key exchange 
    procid_t recvid;
    typename key_exchange_type::buffer_type buffer;
    while(key_exchange.recv(recvid, buffer)) {
      std::copy(buffer.begin(), buffer.end(), 
          std::inserter(key_values, key_values.end()));
    }
    std::sort(key_values.begin(), key_values.end(), 
        sample_sort_impl::pair_key_comparator<Key,Value>());

    rmi.barrier();
  }

  std::vector<std::pair<Key, Value> >& result() {
    return key_values;
  }
};


} // namespace graphlab

#endif
