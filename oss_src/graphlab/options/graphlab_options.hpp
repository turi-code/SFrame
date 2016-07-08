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


/**
 * Also contains code that is Copyright 2011 Yahoo! Inc.  All rights
 * reserved.  
 *
 * Contributed under the iCLA for:
 *    Joseph Gonzalez (jegonzal@yahoo-inc.com) 
 *
 */



#ifndef GRAPHLAB_GRAPHLAB_OPTIONS_HPP
#define GRAPHLAB_GRAPHLAB_OPTIONS_HPP

#include <graphlab/options/options_map.hpp>
 
#include <parallel/pthread_tools.hpp>
namespace graphlab {


  /**
   * The engine options class is really a simple struct that contains
   * the basic options needed to create an engine.  These options
   * include:
   
   <ul>

   <li> size_t ncpus: The number of cpus (threads) to use for this
   engine. </li>

   <li> std::string engine_type: The type of engine to use.  Currently
   we support {async,  synchronous}. </li>

   <li> std::string scheduler_type: The type of scheduler to user.
   Currently we support a wide range of schedulers: {synchronous,
   fifo, priority, sampling, splash,  sweep, multiqueue_fifo,
   multiqueue_priority,  set, clustered_priority, round_robin,
   chromatic} </li>

   <li> size_t splash_size: The size parameter for the splash
   scheduler. </li>
   </ul>
   */
  class graphlab_options {
  public:
    //! The number of cpus
    size_t ncpus;
    
    //! The type of scheduler to use
    std::string scheduler_type;
    
    //! additional arguments to the engine
    options_map engine_args;
    
    //! additional arguments to the scheduler
    options_map scheduler_args;

    //! Options for the graph
    options_map graph_args;

    graphlab_options() :
      ncpus(thread::cpu_count() > 2 ? (thread::cpu_count() - 2) : 2) {
      // Grab all the compiler flags 
      /* \todo: Add these back at some point
        #ifdef COMPILEFLAGS
        #define QUOTEME_(x) #x
        #define QUOTEME(x) QUOTEME_(x)
        compile_flags = QUOTEME(COMPILEFLAGS);
        #undef QUOTEME
        #undef QUOTEME_
        #endif 
      */
    } // end of constructor




    virtual ~graphlab_options() {}


    //! Set the number of cpus
    void set_ncpus(size_t n) { ncpus = n; }

    //! Get the number of cpus
    size_t get_ncpus() const { return ncpus; }

    void set_scheduler_type(const std::string& stype) {
      //! \todo: ADD CHECKING
      scheduler_type = stype;
    }    


    //! Get the type of scheduler
    const std::string& get_scheduler_type() const {
      return scheduler_type;
    }

    //! Get the engine arguments
    const options_map& get_engine_args() const {
      return engine_args;
    }

    //! Get the engine arguments
    options_map& get_engine_args() {
      return engine_args;
    }

    const options_map& get_graph_args() const {
      return graph_args;
    }

    options_map& get_graph_args() {
      return graph_args;
    }     


    const options_map& get_scheduler_args() const {
      return scheduler_args;
    }

    options_map& get_scheduler_args() {
      return scheduler_args;
    }

    /**
     * Display the current engine options
     */
    virtual void print() const {
      std::cout << "GraphLab Options -------------------\n" 
                << "ncpus:       " << ncpus << "\n"
                << "scheduler:   " << scheduler_type << "\n";
      std::cout << "\n";
      std::cout << "Scheduler Options: \n";
      std::cout << scheduler_args;
      std::cout << "Graph Options: \n";
      std::cout << graph_args;
      std::cout << "Engine Options: \n";
      std::cout << engine_args;
      std::cout << std::endl;
    }



  };


  
}
#endif

