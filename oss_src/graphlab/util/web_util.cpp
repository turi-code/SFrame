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

#include <graphlab/util/web_util.hpp>
#include <util/stl_util.hpp>



namespace graphlab {
  namespace web_util {

    std::string url_decode(const std::string& url) {
#define HEXTOI(x) (isdigit(x) ? x - '0' : x - 'W')
      std::string ret_str;
      for (size_t i = 0; i < url.size(); ++i) {
        if (url[i] == '%' && 
            (i+1 < url.size() && isxdigit(url[i+1])) &&
            (i+1 < url.size() && isxdigit(url[i+2]))) {
          const char a = tolower(url[i+1]);
          const char b = tolower(url[i+2]);
          const char new_char = ((HEXTOI(a) << 4) | HEXTOI(b));
          i += 2;
          ret_str.push_back(new_char);
        } else if (url[i] == '+') {
          ret_str.push_back(' ');
        } else {
          ret_str.push_back(url[i]);
        }
      }
#undef HEXTOI
      return ret_str;
    } // end of url decode



    std::map<std::string, std::string> parse_query(const std::string& query) {
      std::vector<std::string> pairs = graphlab::strsplit(query, ",=", true);
      std::map<std::string, std::string> map;
      for(size_t i = 0; i+1 < pairs.size(); i+=2) 
        map[url_decode(pairs[i])] = url_decode(pairs[i+1]);
      return map;
    } // end of parse url query

  } // end of namespace web_util
 
}; // end of namespace GraphLab

