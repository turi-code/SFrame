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


#ifndef GRAPHLAB_EXECUTION_STATUS_HPP
#define GRAPHLAB_EXECUTION_STATUS_HPP

namespace graphlab {

  /**
   * \brief the reasons for execution completion.
   *
   * Because there are several reasons why the graphlab engine might
   * terminate the exec_status value is returned from the start
   * function after completing execution. 
   *
   */
  struct execution_status {
    enum status_enum {
      UNSET,          /** The default termination reason */      
      RUNNING,        /** The engine is currently running */
      TASK_DEPLETION, /**<Execution completed successfully due to
                              task depletion */      
      TIMEOUT,        /**< The execution completed after timing
                              out */
      
      FORCED_ABORT,     /**< the engine was stopped by calling force
                                abort */
      
      EXCEPTION        /**< the engine was stopped by an exception */
    }; // end of enum
    
    // Convenience function.
    static std::string to_string(status_enum es) {
      switch(es) {
        case UNSET: return "engine not run!";
        case RUNNING: return "engine is still running!"; 
        case TASK_DEPLETION: return "task depletion (natural)";
        case TIMEOUT: return "timeout";
        case FORCED_ABORT: return "forced abort";
        case EXCEPTION: return "exception";
        default: return "unknown";
      };
    } // end of to_string
  };



}; // end of namespace graphlab
#endif



