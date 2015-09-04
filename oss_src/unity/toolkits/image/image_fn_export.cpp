/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <unity/lib/image_util.hpp>
#include <unity/lib/toolkit_function_macros.hpp>

namespace graphlab{

namespace image_util{


BEGIN_FUNCTION_REGISTRATION
REGISTER_FUNCTION(load_image, "url", "format")
REGISTER_FUNCTION(load_images, "url", "format", "with_path", "recursive", "ignore_failure", "random_order")
REGISTER_FUNCTION(decode_image, "image")
REGISTER_FUNCTION(decode_image_sarray, "image_sarray")
REGISTER_FUNCTION(resize_image, "image",  "resized_width", "resized_height", "resized_channels", "encode")
REGISTER_FUNCTION(resize_image_sarray, "image_sarray",  "resized_width", "resized_height", "resized_channels")
REGISTER_FUNCTION(vector_sarray_to_image_sarray, "sarray",  "width", "height", "channels", "undefined_on_failure")
REGISTER_FUNCTION(generate_mean, "unity_data")
END_FUNCTION_REGISTRATION



} //namespace image_util 
} //namespace graphlab
