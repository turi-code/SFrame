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


#ifndef GRAPHLAB_SCHEDULER_FACTORY_HPP
#define GRAPHLAB_SCHEDULER_FACTORY_HPP

#include <string>

// Schedulers
#include <graphlab/options/graphlab_options.hpp>
#include <graphlab/scheduler/scheduler_list.hpp>

#include <boost/preprocessor.hpp>


namespace graphlab {
  
  
  /**
   *  helper for constructing graphlab engines.
   **/
  struct scheduler_factory {
    typedef ischeduler ischeduler_type;
   
    /**
     * Construct the a scheduler
     */
    template<typename Scheduler>
    static ischeduler_type* 
    new_scheduler_impl(size_t num_vertices, const graphlab_options& opts) {
      ischeduler_type* scheduler_ptr = 
        new Scheduler(num_vertices, opts);
      ASSERT_TRUE(scheduler_ptr != NULL);
      return scheduler_ptr;
    } // end of new_scheduler

    /**
     * This function returns a new scheduler for a particular engine
     */    
    static ischeduler_type* 
    new_scheduler(size_t num_vertices, const graphlab_options& opts) {
      std::string scheduler_str = opts.get_scheduler_type();
#define __GENERATE_NEW_SCHEDULER__(r_unused, data_unused, i,  elem)     \
      BOOST_PP_EXPR_IF(i, else)                                         \
        if (scheduler_str == BOOST_PP_TUPLE_ELEM(3,0,elem)) {           \
          typedef BOOST_PP_TUPLE_ELEM(3,1,elem)                         \
            scheduler_type;                                             \
          return new_scheduler_impl<scheduler_type>                     \
            ( num_vertices, opts);                                      \
        }      
      // generate the construction calls
      BOOST_PP_SEQ_FOR_EACH_I(__GENERATE_NEW_SCHEDULER__, _, __SCHEDULER_LIST__);
#undef __GENERATE_NEW_SCHEDULER__        
      logstream(LOG_FATAL) 
        << "Invalid scheduler type: " << scheduler_str << std::endl;
      return NULL;
    } // end of new_scheduler

  }; // end of class scheduler_factory

}; // End of namespace graphlab




#endif

