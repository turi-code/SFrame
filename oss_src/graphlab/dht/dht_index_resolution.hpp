/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_DHT_INDEX_RESOLUTION_H_
#define GRAPHLAB_DHT_INDEX_RESOLUTION_H_

#include <rpc/dc.hpp>
#include <rpc/dc_dist_object.hpp>
#include <graphlab/util/token.hpp>
#include <graphlab/util/bitops.hpp>

namespace graphlab {
namespace _DHT {

struct StandardHashResolver {

  // Do we assume we have a lot of data?  This value must be set large
  // enough that 2**n_bits_container_lookup times 2^32 cannot fit in
  // memory at all.
  static constexpr uint machine_lookup_hash_offset            = 96;

  static constexpr uint internal_container_lookup_hash_offset = 64;
  static constexpr uint n_bits_container_lookup               = 16;

  typedef uint16_t InternalTableIndexType;

  static InternalTableIndexType getInternalTableIndex(const WeakToken& key) {
    return (n_bits_container_lookup == bitsizeof(InternalTableIndexType)
            ? InternalTableIndexType(key.hash() >> internal_container_lookup_hash_offset)
            : InternalTableIndexType(bitwise_pow2_mod(key.hash() >> internal_container_lookup_hash_offset,
                                                      n_bits_container_lookup)));
  }

  static uint getMachineIndex(const distributed_control& dc, const WeakToken& key) {
    return (key.hash() >> machine_lookup_hash_offset) % dc.numprocs();
  }
};

}}

#endif
