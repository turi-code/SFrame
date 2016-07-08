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

#ifndef GRAPHLAB_CONDITIONAL_COMBINER_WRAPPER_HPP
#define GRAPHLAB_CONDITIONAL_COMBINER_WRAPPER_HPP
#include <algorithm>
#include <boost/function.hpp>
#include <serialization/oarchive.hpp>
#include <serialization/iarchive.hpp>



namespace graphlab {

  template <typename T>
  struct conditional_combiner_wrapper {
  public:
    bool has_value;
    T value;
    boost::function<void(T&, const T&)> combiner;
    conditional_combiner_wrapper(boost::function<void(T&, const T&)> combiner = NULL) : has_value(false), value(T()), combiner(combiner) {};
    explicit conditional_combiner_wrapper(const T& t,
                                          bool has_value = true)
      :has_value(has_value), value(t) {};
 
    void set_combiner(boost::function<void(T&, const T&)> comb) {
      combiner = comb;
    }

    void set(const T& t) {
      value = t;
      has_value = true;
    }
    void swap(T& t) {
      std::swap(value, t);
      has_value = true;
    }
    void clear() {
      has_value = false;
      value = T();
    }

    bool empty() const {
      return !has_value;
    }

    bool not_empty() const {
      return has_value;
    }
  
  
    conditional_combiner_wrapper& 
    operator+=(const conditional_combiner_wrapper<T>& c) {
      if (has_value && c.has_value) {
        // if we both have value, do the regular +=
        combiner(value, c.value);
      }
      else if (!has_value && c.has_value) {
        // I have no value, but other has value. Use the other
        has_value = true;
        value = c.value;
      }
      return *this;
    }

    conditional_combiner_wrapper& operator+=(const T& c) {
      if (has_value) {
        combiner(value, c);
      }
      else if (!has_value) {
        // I have no value, but other has value. Use the other
        has_value = true;
        value = c;
      }
      return *this;
    }


    void save(oarchive& oarc) const {
      oarc << has_value;
      if (has_value) oarc << value;
    }


    void load(iarchive& iarc) {
      iarc >> has_value;
      if (has_value) iarc >> value;
      else value = T();
    }
  
  };
} // namespace graphlab
#endif
