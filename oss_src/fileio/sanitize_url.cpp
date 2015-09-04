/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <string>
#include <boost/algorithm/string/predicate.hpp>
#include <fileio/sanitize_url.hpp>
#include <fileio/s3_api.hpp>
#include <export.hpp>

namespace graphlab {

EXPORT std::string sanitize_url(std::string url) {
  if (boost::algorithm::starts_with(url, "s3://")) {
    return webstor::sanitize_s3_url(url);
  } else {
    return url;
  }
}

}
