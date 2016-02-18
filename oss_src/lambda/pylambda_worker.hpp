/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_LAMBDA_PYLAMBDA_WORKER_H_
#define GRAPHLAB_LAMBDA_PYLAMBDA_WORKER_H_

#include <string> 

namespace graphlab { namespace lambda {

/** The main function to be called from the python ctypes library to
 *  create a pylambda worker process.
 */
int pylambda_worker_main(const std::string& root_path,
                         const std::string& server_address, int loglevel);

}}

#endif /* _PYLAMBDA_WORKER_H_ */
