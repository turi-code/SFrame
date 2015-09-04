/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

/**
 * @file log_level_setter.hpp
 * Usage:
 * Create a log_level_setter object to change the loglevel as desired.
 * Upon destruction of the object, the loglevel will be reset to the
 * previous logging level.
 *
 * auto e = log_level_setter(LOG_NONE); // quiets the logging that follows
 */

#ifndef GRAPHLAB_LOG_LEVEL_SETTER_HPP
#define GRAPHLAB_LOG_LEVEL_SETTER_HPP

#include <logger/logger.hpp>

/**
 * Class for setting global log level. Unsets log level on destruction.
 */
class log_level_setter {
 private:
  int prev_level;
 public:

  /**
   * Set global log level to the provided log level.
   * \param loglevel desired loglevel. See logger.hpp:97 for a description of
   * each level.
   */
  log_level_setter(int loglevel);

  /**
   * Destructor resets global log level to the previous level.
   */
  ~log_level_setter();
};


#endif

