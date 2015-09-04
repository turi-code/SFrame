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


#ifndef GRAPHLAB_SERIALIZE_GL_STRING_HPP
#define GRAPHLAB_SERIALIZE_GL_STRING_HPP

#include <serialization/iarchive.hpp>
#include <serialization/oarchive.hpp>
#include <serialization/iterator.hpp>
#include <generics/gl_string.hpp>

namespace graphlab {

  namespace archive_detail {

    /// Serialization of gl_string
    template <typename OutArcType>
    struct serialize_impl<OutArcType, gl_string, false> {
      static void exec(OutArcType& oarc, const gl_string& s) {
        size_t length = s.length();
        oarc << length;
        if(length > 0) {
          oarc.write(reinterpret_cast<const char*>(s.data()), (std::streamsize)length);
        }
        DASSERT_FALSE(oarc.fail());
      }
    };


    /// Deserialization of gl_string
    template <typename InArcType>
    struct deserialize_impl<InArcType, gl_string, false> {
      static void exec(InArcType& iarc, gl_string& s) {
        //read the length
        size_t length;
        iarc >> length;
        //resize the string and read the characters
        s.resize(length);
        if(length > 0) {
          iarc.read(&(s[0]), (std::streamsize)length);
        }
        DASSERT_FALSE(iarc.fail());
      }
    };
  }
  
} // namespace graphlab

#endif 

