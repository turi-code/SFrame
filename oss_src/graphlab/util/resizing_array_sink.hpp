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


#ifndef GRAPHLAB_RESIZING_COUNTING_SINK
#define GRAPHLAB_RESIZING_COUNTING_SINK

#include <graphlab/util/charstream.hpp>

namespace graphlab {

  typedef charstream_impl::resizing_array_sink<false> resizing_array_sink;
  
  /**
  Wraps a resizing array sink.
  */
  class resizing_array_sink_ref {
   private:
    resizing_array_sink* ras;
   public:
   

    typedef resizing_array_sink::char_type char_type;
    typedef resizing_array_sink::category category;

    inline resizing_array_sink_ref(resizing_array_sink& ref): ras(&ref) { }
  
    inline resizing_array_sink_ref(const resizing_array_sink_ref& other) :
      ras(other.ras) { }

    inline size_t size() const { return ras->size(); }
    inline char* c_str() { return ras->c_str(); }

    inline void clear() { ras->clear(); }
    /** the optimal buffer size is 0. */
    inline std::streamsize optimal_buffer_size() const { 
      return ras->optimal_buffer_size(); 
    }

    inline void relinquish() { ras->relinquish(); }

    inline void advance(std::streamsize n) { ras->advance(n); }

    
    inline std::streamsize write(const char* s, std::streamsize n) {
      return ras->write(s, n);
    }
  };
  
}
#endif

