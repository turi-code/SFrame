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


#ifndef GRAPHLAB_ATOMIC_OPS_HPP
#define GRAPHLAB_ATOMIC_OPS_HPP

#include <stdint.h>


namespace graphlab {
  /**
   * \ingroup util
     atomic instruction that is equivalent to the following:
     \code
     if (a==oldval) {    
       a = newval;           
       return true;          
     }
     else {
       return false;
    }
    \endcode
  */
  template<typename T>
  bool atomic_compare_and_swap(T& a, T oldval, T newval) {
    return __sync_bool_compare_and_swap(&a, oldval, newval);
  };

  /**
   * \ingroup util
     atomic instruction that is equivalent to the following:
     \code
     if (a==oldval) {    
       a = newval;           
       return true;          
     }
     else {
       return false;
    }
    \endcode
  */
  template<typename T>
  bool atomic_compare_and_swap(volatile T& a, 
                               T oldval, 
                               T newval) {
    return __sync_bool_compare_and_swap(&a, oldval, newval);
  };

  /**
   * \ingroup util
     atomic instruction that is equivalent to the following:
     \code
     if (a==oldval) {    
       a = newval;           
       return oldval;          
     }
     else {
     return a;
    }
    \endcode
  */
  template<typename T>
  T atomic_compare_and_swap_val(T& a, T oldval, T newval) {
    return __sync_val_compare_and_swap(&a, oldval, newval);
  };

  /**
   * \ingroup util
     atomic instruction that is equivalent to the following:
     \code
     if (a==oldval) {    
       a = newval;           
       return oldval;          
     }
     else {
     return a;
    }
    \endcode
  */
  template<typename T>
  T atomic_compare_and_swap_val(volatile T& a, 
                                T oldval, 
                                T newval) {
    return __sync_val_compare_and_swap(&a, oldval, newval);
  };

  /**
   * \ingroup util
     atomic instruction that is equivalent to the following:
     \code
     if (a==oldval) {    
       a = newval;           
       return true;          
     }
     else {
       return false;
    }
    \endcode
  */
  template <>
  inline bool atomic_compare_and_swap(volatile double& a, 
                                      double oldval, 
                                      double newval) {
    volatile uint64_t* a_ptr = reinterpret_cast<volatile uint64_t*>(&a);
    const uint64_t* oldval_ptr = reinterpret_cast<const uint64_t*>(&oldval);
    const uint64_t* newval_ptr = reinterpret_cast<const uint64_t*>(&newval);
    return __sync_bool_compare_and_swap(a_ptr, *oldval_ptr, *newval_ptr);
  };

  /**
   * \ingroup util
     atomic instruction that is equivalent to the following:
     \code
     if (a==oldval) {    
       a = newval;           
       return true;          
     }
     else {
       return false;
    }
    \endcode
  */
  template <>
  inline bool atomic_compare_and_swap(volatile float& a, 
                                      float oldval, 
                                      float newval) {
    volatile uint32_t* a_ptr = reinterpret_cast<volatile uint32_t*>(&a);
    const uint32_t* oldval_ptr = reinterpret_cast<const uint32_t*>(&oldval);
    const uint32_t* newval_ptr = reinterpret_cast<const uint32_t*>(&newval);
    return __sync_bool_compare_and_swap(a_ptr, *oldval_ptr, *newval_ptr);
  };

  /** 
    * \ingroup util
    * \brief Atomically exchanges the values of a and b.
    * \warning This is not a full atomic exchange. Read of a,
    * and the write of b into a is atomic. But the write into b is not.
    */
  template<typename T>
  void atomic_exchange(T& a, T& b) {
    b = __sync_lock_test_and_set(&a, b);
  };

  /** 
    * \ingroup util
    * \brief Atomically exchanges the values of a and b.
    * \warning This is not a full atomic exchange. Read of a,
    * and the write of b into a is atomic. But the write into b is not.
    */
  template<typename T>
  void atomic_exchange(volatile T& a, T& b) {
    b = __sync_lock_test_and_set(&a, b);
  };

  /** 
    * \ingroup util
    * \brief Atomically sets a to the newval, returning the old value
    */
  template<typename T>
  T fetch_and_store(T& a, const T& newval) {
    return __sync_lock_test_and_set(&a, newval);
  };

   /** Atomically sets the max, returning the value of the atomic
    *  prior to setting the max value, or the existing value if
    *  nothing changed.  Atomic equivalent to:
    *
    *   old_max_value = max_value;
    *   max_value = std::max(max_value, new_value);
    *   return old_max_value;
    *
    */
  template<typename T> 
  T atomic_set_max(T& max_value, T new_value) {
    T v = max_value;
    T oldval = v;
    if(v < new_value) { 
      do {
        oldval = atomic_compare_and_swap_val(max_value, v, new_value);

        if(oldval == v) {
          // Change successful
          break;
        } else { 
          // Change not successful, reset.
          v = oldval;
        }
      } while(v < new_value);
    }
    
    return oldval;
  }

   /** Atomically sets the max, returning the value of the atomic
    *  prior to this operation, or the existing value if
    *  nothing changed.  Atomic equivalent to:
    *
    *   old_max_value = max_value;
    *   max_value = std::max(max_value, new_value);
    *   return old_max_value;
    *
    *  \overload
    */
  template<typename T> 
  T atomic_set_max(volatile T& max_value, T new_value) {
    T v = max_value;
    T oldval = v;
    if(v < new_value) { 
      do {
        oldval = atomic_compare_and_swap_val(max_value, v, new_value);

        if(oldval == v) {
          // Change successful
          break;
        } else { 
          // Change not successful, reset.
          v = oldval;
        }
      } while(v < new_value);
    }
    return oldval;
  }

   /** Atomically sets the min, returning the value of the atomic
    *  prior to this operation.  Atomic equivalent to:
    *
    *    old_min_value = min_value; 
    *    min_value = std::min(min_value, new_value);
    *    return old_min_value;
    *
    *  \overload
    */
  template<typename T> 
  T atomic_set_min(T& min_value, T new_value) {
    T v = min_value;
    T oldval = v;
    if(v > new_value) { 
      do {
        oldval = atomic_compare_and_swap_val(min_value, v, new_value);

        if(oldval == v) {
          // Change successful
          break;
        } else { 
          // Change not successful, reset.
          v = oldval;
        }
      } while(v > new_value);
    }
    return oldval;
  }

   /** Atomically sets the min, returning the value of the atomic
    *  prior to this operation.  Atomic equivalent to:
    *
    *    old_min_value = min_value; 
    *    min_value = std::min(min_value, new_value);
    *    return old_min_value;
    *
    *  \overload
    */
  template<typename T> 
  T atomic_set_min(volatile T& min_value, T new_value) {
    T v = min_value;
    T oldval = v;
    if(v > new_value) { 
      do {
        oldval = atomic_compare_and_swap_val(min_value, v, new_value);

        if(oldval == v) {
          // Change successful
          break;
        } else {
          // Change not successful, reset.
          v = oldval;
        }
      } while(v > new_value);
    }
    return oldval;
  }

  /**  Atomically increments the value, and returns the value of the
   *   atomic prior to this operation.   Equivalent to
   *
   *     old_value = value;
   *     value += increment;
   *     return value;
   */
  template<typename T>
  T atomic_increment(T& value, int increment = 1) {
    return __sync_fetch_and_add(&value, increment);
  }

  /**  Atomically increments the value, and returns the value of the
   *   atomic prior to this operation.   Equivalent to
   *
   *     old_value = value;
   *     value += increment;
   *     return value;
   *
   *   \overload
   */
  template<typename T>
  T atomic_increment(volatile T& value, int increment = 1) {
    return __sync_fetch_and_add(&value, increment);
  }


}
#endif

