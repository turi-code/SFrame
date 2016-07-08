/**
 * Copyright (C) 2016 Turi
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


/*
   This files defines the serializer/deserializer for all basic types
   (as well as string and pair)  
*/
#ifndef ARCHIVE_BASIC_TYPES_HPP
#define ARCHIVE_BASIC_TYPES_HPP

#include <string>
#include <serialization/serializable_pod.hpp>
#include <logger/assertions.hpp>
#include <cstdint>

namespace graphlab {
  class oarchive;
  class iarchive;
}


namespace graphlab {
  namespace archive_detail {

    /** Serialization of null terminated const char* strings.
     * This is necessary to serialize constant strings like
     * \code 
     * oarc << "hello world";
     * \endcode
     */
    template <typename OutArcType>
    struct serialize_impl<OutArcType, const char*, false> {
      static void exec(OutArcType& oarc, const char* const& s) {
        // save the length
        // ++ for the \0
        size_t length = strlen(s); length++;
        oarc << length;
        oarc.write(reinterpret_cast<const char*>(s), length);
        DASSERT_FALSE(oarc.fail());
      }
    };




    /// Serialization of fixed length char arrays
    template <typename OutArcType, size_t len>
    struct serialize_impl<OutArcType, char [len], false> {
      static void exec(OutArcType& oarc, const char s[len] ) { 
        size_t length = len;
        oarc << length;
        oarc.write(reinterpret_cast<const char*>(s), length);
        DASSERT_FALSE(oarc.fail());
      }
    };


    /// Serialization of null terminated char* strings
    template <typename OutArcType>
    struct serialize_impl<OutArcType, char*, false> {
      static void exec(OutArcType& oarc, char* const& s) {
        // save the length
        // ++ for the \0
        size_t length = strlen(s); length++;
        oarc << length;
        oarc.write(reinterpret_cast<const char*>(s), length);
        DASSERT_FALSE(oarc.fail());
      }
    };

    /// Deserialization of null terminated char* strings
    template <typename InArcType>
    struct deserialize_impl<InArcType, char*, false> {
      static void exec(InArcType& iarc, char*& s) {
        // Save the length and check if lengths match
        size_t length;
        iarc >> length;
        s = new char[length];
        //operator>> the rest
        iarc.read(reinterpret_cast<char*>(s), length);
        DASSERT_FALSE(iarc.fail());
      }
    };
  
    /// Deserialization of fixed length char arrays 
    template <typename InArcType, size_t len>
    struct deserialize_impl<InArcType, char [len], false> {
      static void exec(InArcType& iarc, char s[len]) { 
        size_t length;
        iarc >> length;
        ASSERT_LE(length, len);
        iarc.read(reinterpret_cast<char*>(s), length);
        DASSERT_FALSE(iarc.fail());
      }
    };



    /// Serialization of std::string
    template <typename OutArcType>
    struct serialize_impl<OutArcType, std::string, false> {
      static void exec(OutArcType& oarc, const std::string& s) {
        size_t length = s.length();
        oarc << length;
        oarc.write(reinterpret_cast<const char*>(s.c_str()), 
                   (std::streamsize)length);
        DASSERT_FALSE(oarc.fail());
      }
    };


    /// Deserialization of std::string
    template <typename InArcType>
    struct deserialize_impl<InArcType, std::string, false> {
      static void exec(InArcType& iarc, std::string& s) {
        //read the length
        size_t length;
        iarc >> length;
        //resize the string and read the characters
        s.resize(length);
        iarc.read(&(s[0]), (std::streamsize)length);
        DASSERT_FALSE(iarc.fail());
      }
    };

    /// Serialization of std::pair
    template <typename OutArcType, typename T, typename U>
    struct serialize_impl<OutArcType, std::pair<T, U>, false > {
      static void exec(OutArcType& oarc, const std::pair<T, U>& s) {
        oarc << s.first << s.second;
      }
    };


    /// Deserialization of std::pair
    template <typename InArcType, typename T, typename U>
    struct deserialize_impl<InArcType, std::pair<T, U>, false > {
      static void exec(InArcType& iarc, std::pair<T, U>& s) {
        iarc >> s.first >> s.second;
      }
    };


// 
//     /** Serialization of 8 byte wide integers
//      * \code 
//      * oarc << vec.length();
//      * \endcode
//      */
//     template <typename OutArcType>
//     struct serialize_impl<OutArcType, unsigned long , true> {
//       static void exec(OutArcType& oarc, const unsigned long & s) {
//         // only bottom 1 byte
//         if ((s >> 8) == 0) {
//           unsigned char c = 0;
//           unsigned char trunc_s = s;
//           oarc.direct_assign(c);
//           oarc.direct_assign(trunc_s);
//         }
//         // only bottom 2 byte
//         else if ((s >> 16) == 0) {
//           unsigned char c = 1;
//           unsigned short trunc_s = s;
//           oarc.direct_assign(c);
//           oarc.direct_assign(trunc_s);
//         }
//         // only bottom 4 byte
//         else if ((s >> 32) == 0) {
//           unsigned char c = 2;
//           uint32_t trunc_s = s;
//           oarc.direct_assign(c);
//           oarc.direct_assign(trunc_s);
//         } 
//         else {
//           unsigned char c = 3;
//           oarc.direct_assign(c);
//           oarc.direct_assign(s);
//         }
//       }
//     };
// 
// 
//     /// Deserialization of 8 byte wide integer 
//     template <typename InArcType>
//     struct deserialize_impl<InArcType, unsigned long , true> {
//       static void exec(InArcType& iarc, unsigned long & s) {
//         unsigned char c;
//         iarc.read(reinterpret_cast<char*>(&c), 1);
//         switch(c) {
//          case 0: {
//            unsigned char val;
//            iarc.read(reinterpret_cast<char*>(&val), 1);
//            s = val;
//            break;
//          }
//          case 1: {
//            unsigned short val;
//            iarc.read(reinterpret_cast<char*>(&val), 2);
//            s = val;
//            break;
//          }
//          case 2: {
//            uint32_t val;
//            iarc.read(reinterpret_cast<char*>(&val), 4);
//            s = val;
//            break;
//          }
//          case 3: {
//            iarc.read(reinterpret_cast<char*>(&s), 8);
//            break;
//          }
//          default:
//            ASSERT_LE(c, 3);
//         };
//       }
//     };
// 


  } // namespace archive_detail
} // namespace graphlab
 
#undef INT_SERIALIZE
#endif

