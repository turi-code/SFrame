/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#ifndef GRAPHLAB_UNITY_BASE_TYPES_HPP
#define GRAPHLAB_UNITY_BASE_TYPES_HPP
#include <memory>
#include <flexible_type/flexible_type.hpp>
#include <sframe/sframe.hpp>
#include <sframe/sarray.hpp>
namespace graphlab {

/// The sframe type
typedef sframe sframe_type;

typedef sarray<flexible_type> sarray_type;

} // namespace graphlab

#endif
