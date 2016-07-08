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


#ifndef GRAPHLAB_UNSUPPORTED_SERIALIZE_HPP
#define GRAPHLAB_UNSUPPORTED_SERIALIZE_HPP

#include <serialization/iarchive.hpp>
#include <serialization/oarchive.hpp>
#include <logger/logger.hpp>

namespace graphlab {

  /**
   * \ingroup group_serialization
   *  \brief Inheritting from this class will prevent the serialization
   *         of the derived class. Used for debugging purposes.
   * 
   *  Inheritting from this class will result in an assertion failure
   * if any attempt is made to serialize or deserialize the derived
   * class. This is largely used for debugging purposes to enforce
   * that certain types are never serialized 
   */
  struct unsupported_serialize {
    void save(oarchive& archive) const {      
      ASSERT_MSG(false, "trying to serialize an unserializable object");
    }
    void load(iarchive& archive) {
      ASSERT_MSG(false, "trying to deserialize an unserializable object");
    }
  }; // end of struct
};


/**
\ingroup group_serialization
\brief A macro which disables the serialization of type so that 
it will fault at runtime. 

Writing GRAPHLAB_UNSERIALIZABLE(T) for some typename T in the global namespace
will result in an assertion failure if any attempt is made to serialize or
deserialize the type T.  This is largely used for debugging purposes to enforce
that certain types are never serialized. 
*/
#define GRAPHLAB_UNSERIALIZABLE(tname) \
  BEGIN_OUT_OF_PLACE_LOAD(arc, tname, tval) \
    ASSERT_MSG(false,"trying to deserialize an unserializable object"); \
  END_OUT_OF_PLACE_LOAD()                                           \
  \
  BEGIN_OUT_OF_PLACE_SAVE(arc, tname, tval) \
    ASSERT_MSG(false,"trying to serialize an unserializable object"); \
  END_OUT_OF_PLACE_SAVE()                                           \


#endif

