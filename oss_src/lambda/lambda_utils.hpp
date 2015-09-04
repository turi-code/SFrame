/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_LAMBDA_LAMBDA_UTILS_HPP
#define GRAPHLAB_LAMBDA_LAMBDA_UTILS_HPP

#include<cppipc/common/message_types.hpp>

namespace graphlab {
namespace lambda {

/**
 * Helper function to convert an communication failure exception to a user
 * friendly message indicating a failure lambda execution.
 */
inline cppipc::ipcexception reinterpret_comm_failure(cppipc::ipcexception e) {
  const char* message = "Fail executing the lambda function. The lambda worker may have run out of memory or crashed because it captured objects that cannot be properly serialized.";
  if (e.get_reply_status() == cppipc::reply_status::COMM_FAILURE) {
    return cppipc::ipcexception(cppipc::reply_status::EXCEPTION,
                                e.get_zeromq_errorcode(),
                                message);
  } else {
    return e;
  }
}

}
}

#endif
