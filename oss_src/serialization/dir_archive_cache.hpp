/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#ifndef GRAPHLAB_SERIALIZATION_DIR_ARCHIVE_CACHE_HPP
#define GRAPHLAB_SERIALIZATION_DIR_ARCHIVE_CACHE_HPP

#include <string>
#include <unordered_map>
#include <parallel/mutex.hpp>

namespace graphlab {

class dir_archive_cache {
 public:
  /**
   * Delete all cached dir archives
   */
  ~dir_archive_cache();

  /**
   * Get the singleton cache manager
   */
  static dir_archive_cache& get_instance();

  /**
   * Return the local directory corresponding to the url.
   *
   * Currently supported url is s3 only.
   */
  std::string get_directory(const std::string& url);

 private:
  struct dir_metadata{
    std::string directory;
    std::string last_modified;
  };

 private:
  std::unordered_map<std::string, dir_metadata> url_to_dir;
  mutex lock;
};

}
#endif
