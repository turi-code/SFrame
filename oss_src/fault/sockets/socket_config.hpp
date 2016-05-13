/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef LIBFAULT_SOCKET_CONFIG_HPP
#define LIBFAULT_SOCKET_CONFIG_HPP
#include <string>
namespace libfault {
extern int SEND_TIMEOUT;
extern int RECV_TIMEOUT;

void set_send_timeout(int ms);
void set_recv_timeout(int ms);

void set_conservative_socket_parameters(void* socket);

extern int64_t FORCE_IPC_TO_TCP_FALLBACK;

/**
 * Normalizes an zeromq address. 
 * On windows, this converts IPc addresses to localhost address.
 */
std::string normalize_address(const std::string& address);
};

#endif
