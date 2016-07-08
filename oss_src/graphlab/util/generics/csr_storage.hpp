/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
/**  
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
#ifndef GRAPHLAB_CSR_STORAGE
#define GRAPHLAB_CSR_STORAGE

#include <iostream>
#include <vector>

#include <graphlab/util/generics/counting_sort.hpp>
#include <serialization/iarchive.hpp>
#include <serialization/oarchive.hpp>

namespace graphlab {
  /**
   * A compact key-value(s) data structure using Compressed Sparse Row format.
   * The key has type size_t and can be assolicated with multiple values of valuetype.
   * The core operation of is querying the list of values associated with the query key *  and returns the begin and end iterators via <code>begin(id)</code>
   * and <code>end(id)</code>.
   */
  template <typename valuetype, typename sizetype=size_t>
  class csr_storage {
   public:
     typedef typename std::vector<valuetype>::iterator iterator;
     typedef typename std::vector<valuetype>::const_iterator const_iterator;
     typedef valuetype value_type;

   public:
     csr_storage() { }

     /**
      * Construct the storage from given id vector and value vector.
      * id_vec and value_vec must have the same size.
      */
     template<typename idtype>
     csr_storage(const std::vector<idtype>& id_vec,
                 const std::vector<valuetype>& value_vec) {
        init(id_vec, value_vec);
     }

     template<typename idtype>
     void init(const std::vector<idtype>& id_vec,
               const std::vector<valuetype>& value_vec) {

      ASSERT_EQ(id_vec.size(), value_vec.size());

      std::vector<sizetype> permute_index;
      // Build index for id -> value 
      // Prefix of the counting array equals to the begin index for each id
      std::vector<sizetype>& prefix = value_ptrs;

      counting_sort(id_vec, permute_index, &prefix);

      values.reserve(value_vec.size());
      values.resize(value_vec.size());
      for (ssize_t i = 0; i < (ssize_t)value_vec.size(); ++i) {
        values[i] = value_vec[permute_index[i]];
      }

#ifdef DEBUG_CSR
      for (size_t i = 0; i < permute_index.size(); ++i)
        std::cerr << permute_index[i] << " ";
      std::cerr << std::endl;

      for (size_t i = 0; i < value_ptrs.size(); ++i)
        std::cerr << value_ptrs[i] << " ";
      std::cerr << std::endl;

      for (size_t i = 0; i < values.size(); ++i)
        std::cerr << values[i] << " ";
      std::cerr << std::endl;

      for (size_t i = 0; i < num_keys(); ++i) {
        std::cerr << i << ": ";
        iterator it = begin(i);
        while (it != end(i)) {
          std::cerr << *it << " "; 
          ++it;
        }
        std::cerr << endl;
      }
      std::cerr << std::endl;
#endif
     }

     /**
      * Wrap the index vector and value vector into csr_storage.
      * Check the property of the input vector.
      * The input vector will be cleared. 
      */
     void wrap(std::vector<sizetype>& valueptr_vec,
               std::vector<valuetype>& value_vec) {
       for (ssize_t i = 1; i < (ssize_t)valueptr_vec.size(); ++i) {
         ASSERT_LE(valueptr_vec[i-1], valueptr_vec[i]);
         ASSERT_LT(valueptr_vec[i], value_vec.size());
       }
       value_ptrs.swap(valueptr_vec);
       values.swap(value_vec);
     }

     /// Number of keys in the storage.
     inline size_t num_keys() const { return value_ptrs.size(); }

     /// Number of values in the storage.
     inline size_t num_values() const { return values.size(); }

     /// Return iterator to the begining value with key == id 
     inline iterator begin(size_t id) {
       return id < num_keys() ? values.begin()+value_ptrs[id] : values.end();
     } 

     /// Return iterator to the ending+1 value with key == id 
     inline iterator end(size_t id) {
       return (id+1) < num_keys() ? values.begin()+value_ptrs[id+1] : values.end();
     }

     /// Return iterator to the begining value with key == id 
     inline const_iterator begin(size_t id) const {
       return id < num_keys() ? values.begin()+value_ptrs[id] : values.end();
     } 

     /// Return iterator to the ending+1 value with key == id 
     inline const_iterator end(size_t id) const {
       return (id+1) < num_keys() ? values.begin()+value_ptrs[id+1] : values.end();
     }

     /// printout the csr storage
     void print(std::ostream& out) {
       for (size_t i = 0; i < num_keys(); ++i)  {
         iterator iter = begin(i);
          out << i << ": ";
         while (iter != end(i)) {
           out << *iter <<  " ";
           ++iter;
         }
         out << std::endl;
       }
     }

   public:
     std::vector<valuetype> get_values() { return values; }
     std::vector<sizetype> get_index() { return value_ptrs; }

     void swap(csr_storage<valuetype, sizetype>& other) {
       value_ptrs.swap(other.value_ptrs);
       values.swap(other.values);
     }

     void clear() {
       std::vector<sizetype>().swap(value_ptrs);
       std::vector<valuetype>().swap(values);
     }

     void load(iarchive& iarc) {
       clear();
       iarc >> value_ptrs
            >> values;
     }
     void save(oarchive& oarc) const {
       oarc << value_ptrs
            << values;
     }

     size_t estimate_sizeof() const {
       return sizeof(value_ptrs) + sizeof(values) + sizeof(sizetype)*value_ptrs.capacity() + sizeof(valuetype) * values.capacity();
     }

   private:
     std::vector<sizetype> value_ptrs;
     std::vector<valuetype> values;
  }; // end of class
} // end of graphlab 
#endif
