/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#ifndef CPPIPC_COMMON_STATUS_TYPES_HPP
#define CPPIPC_COMMON_STATUS_TYPES_HPP
namespace cppipc {

/// Server emits this as the status_type string for informational messages
#define STATUS_COMM_SERVER_INFO "COMM_SERVER_INFO"
/// Server emits this as the status_type string for error messages
#define STATUS_COMM_SERVER_ERROR "COMM_SERVER_ERROR"

/// Client watches on this prefix to get informational messages
#define WATCH_COMM_SERVER_INFO "COMM_SERVER_INFO:"
/// Client watches on this prefix to get error messages
#define WATCH_COMM_SERVER_ERROR "COMM_SERVER_ERROR:"

} // namespace cppipc
#endif
