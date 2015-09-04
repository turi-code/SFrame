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


#include <util/any.hpp>

namespace graphlab {


  /**
   * Define the static registry for any
   */
  any::registry_map_type& any::get_global_registry() {
    static any::registry_map_type global_registry;
    return global_registry;
  }



  any::iholder* any::iholder::load(iarchive_soft_fail &arc) {
    registry_map_type& global_registry = get_global_registry();
    uint64_t idload;
    arc >> idload;
    registry_map_type::const_iterator iter = global_registry.find(idload);
    if(iter == global_registry.end()) {
      logstream(LOG_FATAL) 
        << "Cannot load object with hashed type [" << idload 
        << "] from stream!" << std::endl
        << "\t A possible cause of this problem is that the type" 
        << std::endl
        << "\t is never explicity used in this program.\n\n" << std::endl;
      return NULL;
    }
    // Otherwise the iterator points to the deserialization routine
    // for this type
    return iter->second(arc);
  }
  

} // end of namespace graphlab


std::ostream& operator<<(std::ostream& out, const graphlab::any& any) {
  return any.print(out);
} // end of operator << for any

