/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#include <logger/log_level_setter.hpp>

log_level_setter::log_level_setter(int loglevel) {
  prev_level =  global_logger().get_log_level();
  global_logger().set_log_level(loglevel);
}

log_level_setter::~log_level_setter() {
  global_logger().set_log_level(prev_level);
}


