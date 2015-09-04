/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#ifndef CPPIPC_SERVER_CANCEL_OPS_HPP
#define CPPIPC_SERVER_CANCEL_OPS_HPP
#include <atomic>
#include <iostream>

namespace cppipc {

std::atomic<unsigned long long>& get_srv_running_command();

std::atomic<bool>& get_cancel_bit_checked();

bool must_cancel();

} // cppipc

#endif // CPPIPC_SERVER_CANCEL_OPS_HPP
