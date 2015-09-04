/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
/*  
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


#include <rpc/get_current_process_hash.hpp>
#include <metric/mongoose/mongoose.h>

#ifdef __APPLE__
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <libproc.h>
#include <unistd.h>
#endif
namespace graphlab {
namespace dc_impl {


#ifdef __linux
std::string get_current_process_hash() {
  char buf[33];
  mg_md5_file(buf, "/proc/self/exe");
  buf[32] = '\0';
  std::string ret = buf;
  if (ret.length() != 32) {
    ret = std::string(32, '0');
  }
  return ret;
}
#elif __APPLE__
std::string get_current_process_hash() {
  std::string ret;

  pid_t pid = getpid();
  char pathbuf[PROC_PIDPATHINFO_MAXSIZE];
  int pidsuccess = proc_pidpath (pid, pathbuf, sizeof(pathbuf));
  if (pidsuccess > 0) {
    char buf[33];
    mg_md5_file(buf,  pathbuf);
    buf[32] = '\0';
    ret = buf;
  }
  if (ret.length() != 32) {
    ret = std::string(32, '0');
  }
  return ret;
}
#endif

} // dc_impl
} // graphlab
