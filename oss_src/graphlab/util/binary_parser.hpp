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


#ifndef GRAPHLAB_BINARY_PARSER_HPP
#define GRAPHLAB_BINARY_PARSER_HPP

#include <iostream>
#include <fstream>

namespace graphlab {

  /**
   * \ingroup util_internal
   * A thin wrapper around ifstream to provide simplicity of reading
   * of binary data.
   * \see binary_output_stream
   */
  class binary_input_stream : public std::ifstream {
    typedef std::ifstream base_type;
    using base_type::bad;
  public:
    binary_input_stream(const char* fname) :
      base_type(fname, std::ios::binary | std::ios::in) {
      assert(bad() == false);
    }
    
    /**
     * Read an arbitrary type.
     */
    template<typename T> T read() {
      T t;
      base_type::read(reinterpret_cast<char*>(&t), sizeof(T));
      if(bad()) {
        std::cerr << "Error reading file!" << std::endl;
        assert(false);
      }
      return t;
    }
    /**
     * Read an arbitrary type.
     */
    template<typename T> void read(T& ret) {
      base_type::read(reinterpret_cast<char*>(&ret), sizeof(T));
      if(bad()) {
        std::cerr << "Error reading file!" << std::endl;
        assert(false);
      }
    }

    /**
     * Read an arbitrary type.
     */
    template<typename T> void read_vector(std::vector<T>& ret) {
      if(ret.empty()) return;
      base_type::read(reinterpret_cast<char*>(&(ret[0])), 
                      sizeof(T) * ret.size());
      if(bad()) {
        std::cerr << "Error reading file!" << std::endl;
        assert(false);
      }
    }
  };



  /**
   * \ingroup util_internal
   * A thin wrapper around ifstream to provide simplicity of writing
   * of binary data.
   * \see binary_input_stream
   */
  class binary_output_stream : public std::ofstream {
  typedef std::ofstream base_type;
    using std::ofstream::bad;
  public:
    binary_output_stream(const char* fname) : 
    std::ofstream(fname, std::ios::binary | std::ios::out) {
      assert(bad() == false);
    }
    
    //! Write the arbitrary data type to file
    template<typename T> void write(T t) {
      base_type::write(reinterpret_cast<char*>(&t), sizeof(T));
      if(bad()) {
        std::cerr << "Error writing file!" << std::endl;
        assert(false);
      }
    }
  }; // end of binary_output_stream

  

}




#endif




