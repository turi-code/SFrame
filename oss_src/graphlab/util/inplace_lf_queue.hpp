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


#ifndef GRAPHLAB_INPLACE_LOCKFREE_QUEUE_HPP
#define GRAPHLAB_INPLACE_LOCKFREE_QUEUE_HPP
#include <stdint.h>
#include <cstring>
#include <parallel/atomic.hpp>
#include <parallel/atomic_ops.hpp>
#include <utility>
namespace graphlab {

/*
 * A lock free queue where each element is a byte sequence,
 * where the first 8 bytes can be used for a next pointer.
 *
 * head is the head of the queue. Always sentinel.
 * tail is current last element of the queue.
 * completed is the last element that is completely inserted.
 * There can only be one thread dequeueing.
 *
 * On dequeue_all, the dequeu-er should use get_next() to get the
 * next element in the list. If get_next() returns NULL, it should spin
 * until not null, and quit only when end_of_dequeue_list() evaluates to true
 */
class inplace_lf_queue {
 public:
   inline inplace_lf_queue():head(sentinel),tail(sentinel) {
     for (size_t i = 0;i < sizeof(size_t); ++i) sentinel[i] = 0;
   }

   void enqueue(char* c);

   void enqueue_unsafe(char* c);

   char* dequeue_all();

   char* dequeue_all_unsafe();

   static inline char* get_next(char* ptr) {
     return *(reinterpret_cast<char**>(ptr));
   }

   static inline char** get_next_ptr(char* ptr) {
     return reinterpret_cast<char**>(ptr);
   }

   inline const bool end_of_dequeue_list(char* ptr) {
     return ptr == sentinel;
   }

 private:

   char sentinel[sizeof(size_t)];
   char* head;
   char* tail;

   char cache_line_padding[64 - 24];
};


} // namespace graphlab

#endif
