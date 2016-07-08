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


#ifndef GRAPHLAB_HASHSTREAM
#define GRAPHLAB_HASHSTREAM

#include <boost/iostreams/stream.hpp>
#include <boost/iostreams/categories.hpp>

namespace graphlab {

  /// \ingroup util_internal
  namespace hashstream_impl {
    /// \ingroup util_internal
    struct hashstream_sink {
      size_t hash;
      size_t len;

      inline hashstream_sink(size_t unused = 0):hash(0),len(0) { }

      inline hashstream_sink(const hashstream_sink& other) :
        hash(other.hash),len(other.len) { }

      inline ~hashstream_sink() { }

      size_t size() const { return len; }
      char* c_str() { return NULL; }
      const char* c_str() const { return NULL; }

      void clear() {
        len = 0;
        hash = 0;
      }

      void reserve(size_t new_buffer_size) { }

      typedef char        char_type;
      struct category: public boost::iostreams::device_tag,
                       public boost::iostreams::output,
                       public boost::iostreams::multichar_tag { };

      /** the optimal buffer size is 0. */
      inline std::streamsize optimal_buffer_size() const { return 0; }

      inline std::streamsize advance(std::streamsize n) {
        len += n;
        return n;
      }

      inline std::streamsize write(const char* s, std::streamsize n) {
        for (size_t i = 0;i < (size_t)n; ++i) {
          hash = hash * 101 + s[i];
        }
        len += n;
        return n;
      }

      inline void swap(hashstream_sink &other) {
        std::swap(hash, other.hash);
        std::swap(len, other.len);
      }

    };

  }; // end of impl;


  typedef boost::iostreams::stream< hashstream_impl::hashstream_sink >
  hashstream;


}; // end of namespace graphlab
#endif

