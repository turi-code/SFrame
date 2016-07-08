/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#ifndef FILEIO_SANITIZE_URL_HPP
#define FILEIO_SANITIZE_URL_HPP
#include <string>
namespace graphlab {
/**
 * Sanitizes a general_fstream URL so that it is suitable for printing;
 * right now, all it does is to drop all credential information when
 * the protocol is s3.
 */
std::string sanitize_url(std::string url);
}
#endif
