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

typedef int(*sarray_callback_type)(const graphlab::flexible_type*, void*);
typedef int(*sframe_callback_type)(const graphlab::flexible_type*, size_t, void*);

/**
 * Apply callback function F on sarray[begin:end] in sequence.
 * The type of the callback function must be int F(const flexible_type* element, void* callback_data);
 * F must return 0 on success.
 *
 * Throw string exception with callback return code if the callback returns non-zero. 
 */
void sarray_callback(graphlab::gl_sarray input, size_t callback_fun_ptr, size_t callback_data_ptr,
                     size_t begin, size_t end);

/**
 * Apply callback function F on sframe[begin:end] in sequence.
 * The type of the callback function must be int F(const flexible_type* row, size_t row_size, void* callback_data);
 * F must return 0 on success.
 *
 * Throw string exception with callback return code if the callback returns non-zero. 
 */
void sframe_callback(graphlab::gl_sframe input, size_t callback_fun_ptr,
                     size_t callback_data_ptr, size_t begin, size_t end);

#endif
