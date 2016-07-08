/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#ifndef GRAPHLAB_LOGGER_LOG_ROTATE_HPP
#define GRAPHLAB_LOGGER_LOG_ROTATE_HPP
#include <cstddef>
#include <string>
namespace graphlab {

/**
 * Sets up log rotation.
 * The basic procedure is that it will generate files of the form
 *
 * \verbatim
 *   [log_file_name].0
 *   [log_file_name].1
 *   [log_file_name].2
 *   etc.
 * \endverbatim
 *
 * When truncate_limit is set, a maximum number of files is maintained.
 * Beyond which, older files are deleted.
 *
 * A symlink [log_file_name].current is also created which always points to the
 * most recent log file.
 * 
 * If log rotation has already been set up, this will stop
 * the the log rotation and begin a new one.
 *
 * Not safe for concurrent use.
 *
 * \param log_file_name The prefix to output to. Logs will emit to 
 *                      [log_file_name].0, [log_file_name].1, etc.
 * \param log_interval  The number of seconds between rotations
 * \param truncate_limit The maximum number of files to maintain. Must be >= 1
 */
void begin_log_rotation(std::string log_file_name, 
                        size_t log_interval, 
                        size_t truncate_limit);

/**
 * Stops log rotation. 
 *
 * No-op if log rotation was not started.
 * 
 * Not safe for concurrent use.
 */
void stop_log_rotation();

} // graphlab
#endif // GRAPHLAB_LOGGER_LOG_ROTATE_HPP

