/**
 * Copyright (C) 2015 Dato, Inc.
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

#ifndef GRAPHLAB_WEB_UTIL_HPP
#define GRAPHLAB_WEB_UTIL_HPP

#include <string>
#include <map>


namespace graphlab {
  namespace web_util {

    /**
     * \brief decode a url by converting escape characters
     */
    std::string url_decode(const std::string& url); 

    /**
     * \brief convert a query string into a map
     */
    std::map<std::string, std::string> parse_query(const std::string& query);

  } // end of namespace web_util
 
}; // end of namespace GraphLab
#endif





