/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_FILEIO_RUN_AWS_HPP
#define GRAPHLAB_FILEIO_RUN_AWS_HPP

#include <string>
#include <vector>

namespace graphlab {
  namespace fileio {
/**
 * Helper function to launch an external aws-cli command.
 * Assuming aws-cli is installed and is generally accessible.
 *
 * The current thread will fork and execute the aws-cli,
 * block until the child process completes.
 *
 * Intermediate output to stdout is piped to logprogress_stream.
 * Any message written by child process to stderr is captured
 * and return to the caller.
 *
 * In the success case, this function should return an empty string.
 */
std::string run_aws_command(const std::vector<std::string>& arglist,
                            const std::string& aws_access_key_id,
                            const std::string& aws_secret_access_key);

} // end of fileio
} // end of graphlab 


#endif
