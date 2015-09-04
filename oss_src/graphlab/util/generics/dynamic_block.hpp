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


#ifndef GRAPHLAB_DYNAMIC_BLOCK_HPP
#define GRAPHLAB_DYNAMIC_BLOCK_HPP

#include <stdint.h>
#include <logger/assertions.hpp>

namespace graphlab {
  template<typename valuetype, uint32_t capacity>
  class block_linked_list;

  /**
   * Define a block storage with valuetype and fixed capacity.
   */
  template<typename valuetype, uint32_t capacity=(4096-20)/sizeof(valuetype)>
  class dynamic_block {
   public:

     /// construct empty block
     dynamic_block() : _next(NULL), _size(0) { }

     template<typename InputIterator>
     void assign(InputIterator first, InputIterator last) {
       size_t len = last-first; 
       ASSERT_LE(len, capacity);
       _size = last-first;
       int i = 0;
       InputIterator iter = first;
       while (iter != last) {
         values[i++] = *iter;
         iter++;
       }
     }

     /// split the block into two parts
     void split() {
       // create new block
       dynamic_block* secondhalf = new dynamic_block();
       // copy the second half over
       uint32_t mid = capacity/2;
       memcpy(secondhalf->values, &values[mid], (capacity/2)*sizeof(valuetype));
       // update pointer
       secondhalf->_next = _next;
       _next = secondhalf;
       _size = capacity/2;
       secondhalf->_size = capacity/2;
     }

     /// return the ith element in the block
     valuetype& get(uint32_t i) {
       ASSERT_LT(i, _size);
       return values[i];
     }

     /// add a new element in to the end of the block
     /// return false when the block is full
     bool try_add(const valuetype& elem) {
       if (_size == capacity) {
         return false;
       } else {
         values[_size++] = elem;
         return true;
       }
     }

     inline bool is_full() const { return _size == capacity; }

     /// insert an element at pos, move elements after pos by 1.
     /// return false when the block is full
     bool insert(const valuetype& elem, uint32_t pos) {
       if (is_full()) {
         return false;
       } else {
         if (pos < _size)
           memmove(values+pos+1, values+pos, (_size-pos)*sizeof(valuetype));
         values[pos] = elem;
         ++_size;
         return true;
       }
     }

     /// returns the size of the block
     size_t size() const {
       return _size;
     }

     dynamic_block* next() {
       return _next;
     }

     void clear() {
       _size = 0;
     }

   //////////////////// Pretty print API ///////////////////////// 
   void print(std::ostream& out) {
     for (size_t i = 0; i < _size; ++i) {
       out << values[i] << " ";
     }
     if (_size < capacity) {
       out << "_" << (capacity-_size) << " ";
     }
   }
   
   private:
     /// value storage
     valuetype values[capacity];
     /// pointer to the next block
     dynamic_block* _next;
     /// size of the block
     uint32_t _size;

     friend class block_linked_list<valuetype, capacity>;
  };
}// end of namespace
#endif
