/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#ifndef GRAPHLAB_UTIL_CRASH_HANDLER_HPP
#define GRAPHLAB_UTIL_CRASH_HANDLER_HPP

#include <string>
#include <signal.h>

// The filename which we write backtrace to, default empty and we write to STDERR_FILENO
extern std::string BACKTRACE_FNAME;

void crit_err_hdlr(int sig_num, siginfo_t * info, void * ucontext);

#endif
