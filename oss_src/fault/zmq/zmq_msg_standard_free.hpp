/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef ZMQ_MSG_STANDARD_FREE_HPP
#define ZMQ_MSG_STANDARD_FREE_HPP

namespace libfault {

void zmq_msg_standard_free(void* buf, void* hint);

} // libfault

#endif
