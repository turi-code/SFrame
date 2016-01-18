/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#ifndef PROCESS_UTIL_HPP
#define PROCESS_UTIL_HPP

#include<string>

namespace graphlab {

size_t get_parent_pid();
size_t get_my_pid();
void wait_for_parent_exit(size_t parent_pid);


/*
 * Returns true if process is running
 */
bool is_process_running(size_t pid);

/*
 * Returns the environment variable's value
 * or empty string if doesn't exist.
 *
 * Note: on windows, the length of the return
 * value is limited to 65534.
 */
std::string getenv_str(const char* variable_name);

} // namespace graphlab
#endif // PROCESS_UTIL_HPP 
