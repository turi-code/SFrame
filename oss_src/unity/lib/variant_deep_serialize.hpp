/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#ifndef GRAPHLAB_VARIANT_DEEP_SERIALIZE_H_
#define GRAPHLAB_VARIANT_DEEP_SERIALIZE_H_

#include <unity/lib/variant.hpp>

namespace graphlab {

/**
 * Serialize the variant type, deep copying the pointer types.
 */
void variant_deep_save(const variant_type& v, oarchive& oarc);

/**
 *  Overload of above for types castable to and from variant_type
 */
template <typename T>
void variant_deep_save(const T& v, oarchive& oarc) {

  static uint64_t type_check_hash = hash64(0);

  oarc << type_check_hash;

  variant_deep_save(to_variant(v), oarc);
}

/**
 * Deserialize the variant type, allocate new resources for the pointer types.
 */
void variant_deep_load(variant_type& v, iarchive& iarc);

/**
 *  Overload of above for types castable to and from variant_type
 */
template <typename T>
void variant_deep_load(T& v, iarchive& iarc) {

  static uint64_t type_check_hash = hash64(0);

  uint64_t type_check_hash_load;
  iarc >> type_check_hash_load;

  ASSERT_MSG(type_check_hash == type_check_hash_load,
             (std::string("Deserialization of type ") + typeid(T).name()
              + " failed, likely due to corruption earlier in the stream. ").c_str());

  variant_type vv;
  variant_deep_load(vv, iarc);
  v = variant_get_value<T>(vv);
}

} // namespace graphlab 

#endif /* _VARIANT_DEEP_SERIALIZE_H_ */
