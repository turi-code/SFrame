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


#ifndef CHAR_COUNTING_SINK
#define CHAR_COUNTING_SINK
#include <boost/iostreams/stream.hpp>
#include <boost/iostreams/categories.hpp>

namespace graphlab {
  
/**
 \ingroup util_internal
A boost sink device which counts the number of characters written
*/
struct char_counting_sink {
  char_counting_sink(size_t initial = 0):count(initial) { }
  
  char_counting_sink(const char_counting_sink &buf):count(buf.count) { }
  size_t count;
  typedef char        char_type;
  struct category: public boost::iostreams::device_tag,
                       public boost::iostreams::output,
                       public boost::iostreams::multichar_tag,
                       public boost::iostreams::optimally_buffered_tag { };

 /** the optimal buffer size is 0. */
  inline std::streamsize optimal_buffer_size() const { return 0; }

  inline std::streamsize write(const char* s, std::streamsize n) {
    count += n;
    return n;
  }
};
}
#endif

