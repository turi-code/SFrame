/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_RPC2_DC_REGISTRY_HPP
#define GRAPHLAB_RPC2_DC_REGISTRY_HPP
#include <utility>
#include <cstddef>
#include <stdint.h>
namespace graphlab {
typedef uint32_t function_dispatch_id_type;
namespace dc_impl {


function_dispatch_id_type add_to_function_registry(const void* c, size_t len);

std::pair<const void*, size_t> get_from_function_registry_impl(function_dispatch_id_type id);

template <typename F>
inline F get_from_function_registry(function_dispatch_id_type id) {
  std::pair<const void*, size_t> val = get_from_function_registry_impl(id);
  return *reinterpret_cast<F*>(const_cast<void*>(val.first));
}

}
}
#endif
