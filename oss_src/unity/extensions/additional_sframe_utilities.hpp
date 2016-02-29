/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_UNITY_EXTENSIONS_ADDITIONAL_SFRAME_UTILITIES_HPP
#define GRAPHLAB_UNITY_EXTENSIONS_ADDITIONAL_SFRAME_UTILITIES_HPP
#include <unity/lib/gl_sarray.hpp>

void sarray_callback(graphlab::gl_sarray input, size_t callback_addr);

void sframe_callback(graphlab::gl_sframe input, size_t callback_addr);

#endif
