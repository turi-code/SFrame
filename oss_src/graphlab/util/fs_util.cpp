/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
/**  
 * Copyright (c) 2009 Carnegie Mellon University. 
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS
 *  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied.  See the License for the specific language
 *  governing permissions and limitations under the License.
 *
 * For more about this software visit:
 *
 *      http://www.graphlab.ml.cmu.edu
 *
 */


#include <boost/version.hpp>
#include <boost/filesystem.hpp>
#include <boost/algorithm/string/predicate.hpp>


#include <vector>
#include <iostream>
#include <iomanip>
#include <sstream>
#include <string>




#include <graphlab/util/fs_util.hpp>

bool is_hidden(const std::string path) {
  if (path.length() && path[0] == '.') {
    return true;
  }
  return false;
}

void graphlab::fs_util::
list_files_with_suffix(const std::string& pathname,
                       const std::string& suffix,
                       std::vector<std::string>& files,
                       bool ignore_hidden) {
  namespace fs = boost::filesystem;
  fs::path dir_path(pathname);
  fs::directory_iterator end_iter;
  files.clear();
  if ( fs::exists(dir_path) && fs::is_directory(dir_path)) {
    for( fs::directory_iterator dir_iter(dir_path) ; 
         dir_iter != end_iter ; ++dir_iter) {
      if (fs::is_regular_file(dir_iter->status()) ) {
#if BOOST_FILESYSTEM_VERSION >= 3 
        const std::string filename = dir_iter->path().filename().string();
#else
        const std::string filename = dir_iter->leaf();
#endif
        if (suffix.size() > 0 && !boost::ends_with(filename, suffix)) 
          continue;
        if (!ignore_hidden || !is_hidden(filename)) {
          files.push_back(filename);
        }
      }
    }
  }
  std::sort(files.begin(), files.end());
//   namespace fs = boost::filesystem;
//   fs::path path(pathname);
//   assert(fs::exists(path));
//   for(fs::directory_iterator iter( path ), end_iter; 
//       iter != end_iter; ++iter) {
//     if( ! fs::is_directory(iter->status()) ) {

// #if BOOST_FILESYSTEM_VERSION >= 3
//       std::string filename(iter->path().filename().string());
// #else
//       std::string filename(iter->path().filename());
// #endif
//       size_t pos = 
//         filename.size() >= suffix.size()?
//         filename.size() - suffix.size() : 0;
//       std::string ending(filename.substr(pos));
//       if(ending == suffix) {
// #if BOOST_FILESYSTEM_VERSION >= 3
//         files.push_back(iter->path().filename().string());
// #else
//         files.push_back(iter->path().filename());
// #endif
//       }
//     }
//   }
//  std::sort(files.begin(), files.end());
} // end of list files with suffix  



void graphlab::fs_util::
list_files_with_prefix(const std::string& pathname,
                       const std::string& prefix,
                       std::vector<std::string>& files,
                       bool ignore_hidden) {
  namespace fs = boost::filesystem;  
  fs::path dir_path(pathname);
  fs::directory_iterator end_iter;
  files.clear();
  if ( fs::exists(dir_path) && fs::is_directory(dir_path)) {
    for( fs::directory_iterator dir_iter(dir_path) ; 
         dir_iter != end_iter ; ++dir_iter) {
      if (fs::is_regular_file(dir_iter->status()) ) {
        const std::string filename = dir_iter->path().filename().string();
        if (prefix.size() > 0 && !boost::starts_with(filename, prefix)) {
          continue;
        }
        if (!ignore_hidden || !is_hidden(dir_iter->path().leaf().string())) {
          files.push_back(dir_iter->path().string());
        }
      }
    }
  }
  std::sort(files.begin(), files.end());
} // end of list files with prefix





std::string graphlab::fs_util::
change_suffix(const std::string& fname,
              const std::string& new_suffix) {             
  size_t pos = fname.rfind('.');
  assert(pos != std::string::npos); 
  const std::string new_base(fname.substr(0, pos));
  return new_base + new_suffix;
} // end of change_suffix


