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


#ifndef GRAPHLAB_DISTRIBUTED_AGGREGATOR
#define GRAPHLAB_DISTRIBUTED_AGGREGATOR

#ifndef __NO_OPENMP__
#include <omp.h>
#endif

#include <map>
#include <set>
#include <string>
#include <vector>
#include <rpc/dc_dist_object.hpp>
#include <graphlab/vertex_program/icontext.hpp>
#include <graph/distributed_graph.hpp>
#include <graphlab/util/generics/conditional_addition_wrapper.hpp>
#include <graphlab/util/generics/test_function_or_functor_type.hpp>

#include <util/any.hpp>
#include <timer/timer.hpp>
#include <graphlab/util/mutable_queue.hpp>
#include <logger/assertions.hpp>
#include <parallel/pthread_tools.hpp>
#include <graphlab/macros_def.hpp>
namespace graphlab {

  /**
   * \internal
   * Implements a distributed aggregator interface which can be plugged
   * into the engine. This class includes management of periodic aggregators.
   * 
   * Essentially, the engine should ideally pass-through all calls to
   *  - add_vertex_aggregator()
   *  - add_edge_aggregator()
   *  - aggregate_now()
   *  - aggregate_periodic()
   * 
   * On engine start(), the engine should call aggregate_all_periodic() 
   * to ensure all periodic aggregators are called once prior to vertex program
   * execution. After which, the start() function should be called to prepare
   * the state of the schedule. At termination of the engine, the stop()
   * function should be called to reset the state of the aggregator.
   * 
   * During engine execution, two modes of operations are permitted: 
   * synchronous, and asynchronous. In a synchronous mode of execution,
   * the tick_synchronous() function should be called periodically by 
   * exactly one thread on each machine, at the same time. In an asynchronous
   * mode of execution, tick_asynchronous() should be called periodically
   * on each machine by some arbitrary thread. This polls the state of the 
   * schedule and activates aggregation jobs which are ready. 
   * 
   * tick_synchronous() and tick_asynchronous() should not be used 
   * simultaneously within the same engine execution . For details on their 
   * usage, see their respective documentation.
   * 
   */
  template<typename Graph, typename IContext>
  class distributed_aggregator {
  public:
    typedef Graph graph_type;
    typedef typename graph_type::local_edge_list_type local_edge_list_type;
    typedef typename graph_type::local_edge_type local_edge_type;
    typedef typename graph_type::edge_type edge_type;
    typedef typename graph_type::local_vertex_type local_vertex_type;
    typedef typename graph_type::vertex_type vertex_type ;
    typedef IContext icontext_type;

    dc_dist_object<distributed_aggregator> rmi;
    graph_type* graph;
    icontext_type* context;
    
  private:
    
    /**
     * \internal
     * A base class which contains a "type-free" specification of the
     * reduction operation, thus allowing the aggregation to be performs at
     * runtime with no other type information whatsoever.
     */
    struct imap_reduce_base {
      /** \brief makes a copy of the current map reduce spec without copying 
       *         accumulator data     */
      virtual imap_reduce_base* clone_empty() const = 0;
      
      /** \brief Performs a map operation on the given vertex adding to the
       *         internal accumulator */
      virtual void perform_map_vertex(icontext_type&, vertex_type&) = 0;
                                      
      /** \brief Performs a map operation on the given edge adding to the
       *         internal accumulator */
      virtual void perform_map_edge(icontext_type&, edge_type&) = 0;
                                    
      /** \brief Returns true if the accumulation is over vertices. 
                 Returns false if it is over edges.*/
      virtual bool is_vertex_map() const = 0;      
      
      /** \brief Returns the accumulator stored in an any. 
                 (by some magic, any's can be serialized) */
      virtual any get_accumulator() const = 0;
      
      /** \brief Combines accumulators using a second accumulator 
                 stored in an any (as returned by get_accumulator).
                 Must be thread safe.*/
      virtual void add_accumulator_any(any& other) = 0;

      /** \brief Sets the value of the accumulator
                 from an any (as returned by get_accumulator).
                 Must be thread safe.*/
      virtual void set_accumulator_any(any& other) = 0;

      
      /** \brief Combines accumulators using a second accumulator 
                 stored in a second imap_reduce_base class). Must be
                 thread safe. */
      virtual void add_accumulator(imap_reduce_base* other) = 0;
      
      /** \brief Resets the accumulator */
      virtual void clear_accumulator() = 0;
      
      /** \brief Calls the finalize operation on internal accumulator */
      virtual void finalize(icontext_type&) = 0;

      virtual ~imap_reduce_base() { }
    };
    
    template <typename ReductionType>
    struct default_map_types{
      typedef ReductionType (*vertex_map_type)(icontext_type&, const vertex_type&);
      typedef ReductionType (*edge_map_type)(icontext_type&, const edge_type&);
    };

    /**
     * \internal
     * A templated implementation of the imap_reduce_base above.
     * \tparam ReductionType The reduction type. (The type the map function
     *                        returns)
     */
    template <typename ReductionType, 
              typename VertexMapperType,
              typename EdgeMapperType,
              typename FinalizerType>
    struct map_reduce_type : public imap_reduce_base {
      conditional_addition_wrapper<ReductionType> acc;
      VertexMapperType map_vtx_function;
      EdgeMapperType map_edge_function;
      FinalizerType finalize_function;
      
      bool vertex_map;
      mutex lock;
      
      /**
       * \brief Constructor which constructs a vertex reduction
       */
      map_reduce_type(VertexMapperType map_vtx_function,
                      FinalizerType finalize_function)
                : map_vtx_function(map_vtx_function),
                  finalize_function(finalize_function), vertex_map(true) { }

      /**
       * \brief Constructor which constructs an edge reduction. The last bool
       * is unused and allows for disambiguation between the two constructors
       */
      map_reduce_type(EdgeMapperType map_edge_function,
                      FinalizerType finalize_function,
                      bool)
                : map_edge_function(map_edge_function),
                finalize_function(finalize_function), vertex_map(false) { }


      void perform_map_vertex(icontext_type& context, vertex_type& vertex) {
        /** 
         * A compiler error on this line is typically due to the
         * aggregator map function not having the correct type. 
         *
         * Verify that the map function has the following form:
         *
         *  ReductionType mapfun(icontext_type& context, const vertex_type& vertex);
         *
         * It is also possible the accumulator type 
         */
        ReductionType temp = map_vtx_function(context, vertex);
        /**
         * A compiler error on this line is typically due to the
         * accumulator (ReductionType of the map function not having an
         * operator+=.  Ensure that the following is available:
         *
         *   ReductionType& operator+=(ReductionType& lvalue, 
         *                             const ReductionType& rvalue);
         */
        acc += temp;
      } // end of perform_map_vertex
      
      void perform_map_edge(icontext_type& context, edge_type& edge) {
        /** 
         * A compiler error on this line is typically due to the
         * aggregator map function not having the correct type. 
         *
         * Verify that the map function has the following form:
         *
         *  ReductionType mapfun(icontext_type& context, const edge_type& vertex);
         *
         * It is also possible the accumulator type 
         */
        ReductionType temp = map_edge_function(context, edge);
        /**
         * A compiler error on this line is typically due to the
         * accumulator (ReductionType of the map function not having an
         * operator+=.  Ensure that the following is available:
         *
         *   ReductionType& operator+=(ReductionType& lvalue, 
         *                             const ReductionType& rvalue);
         */
        acc += temp; 
      } // end of perform_map_edge
      
      bool is_vertex_map() const {
        return vertex_map;
      }
      
      any get_accumulator() const {
        return any(acc);
      }
      
      void add_accumulator_any(any& other) {
        lock.lock();
        acc += other.as<conditional_addition_wrapper<ReductionType> >();
        lock.unlock();
      }

      void set_accumulator_any(any& other) {
        lock.lock();
        acc = other.as<conditional_addition_wrapper<ReductionType> >();
        lock.unlock();
      }


      void add_accumulator(imap_reduce_base* other) {
        lock.lock();
        acc += dynamic_cast<map_reduce_type*>(other)->acc;
        lock.unlock();
      }

      void clear_accumulator() {
        acc.clear();
      }

      void finalize(icontext_type& context) {
        finalize_function(context, acc.value);
      }
      
      imap_reduce_base* clone_empty() const {
        map_reduce_type* copy;
        if (is_vertex_map()) {
          copy = new map_reduce_type(map_vtx_function,
                                     finalize_function);
        }
        else {
          copy = new map_reduce_type(map_edge_function,
                                     finalize_function,
                                     true);
        }
        return copy;
      }
    };
    

    std::map<std::string, imap_reduce_base*> aggregators;
    std::map<std::string, float> aggregate_period;

    struct async_aggregator_state {
      /// Performs reduction of all local threads. On machine 0, also
      /// accumulates for all machines.
      imap_reduce_base* root_reducer;
      /// Accumulator used for each thread
      std::vector<imap_reduce_base*> per_thread_aggregation;
      /// Count down the completion of the local machine threads
      atomic<int> local_count_down;
      /// Count down the completion of machines. Used only on machine 0
      atomic<int> distributed_count_down;
    };
    std::map<std::string, async_aggregator_state> async_state;

    float start_time;
    
    /* annoyingly the mutable queue is a max heap when I need a min-heap
     * to track the next thing to activate. So we need to keep 
     *  negative priorities... */
    mutable_queue<std::string, float> schedule;
    mutex schedule_lock;
    size_t ncpus;

    template <typename ReductionType, typename F>
    static void test_vertex_mapper_type(std::string key = "") {
      bool test_result = test_function_or_const_functor_2<F,
                                             ReductionType(icontext_type&,
                                                          const vertex_type&),
                                             ReductionType,
                                             icontext_type&,
                                             const vertex_type&>::value;
      if (!test_result) {
        std::stringstream strm;
        strm << "\n";
        if (key.empty()) {
          strm << "Vertex Map Function does not pass strict runtime type checks. \n";
        }
        else {
          strm << "Map Function in Vertex Aggregator " << key
              << " does not pass strict runtime type checks. \n";
        }
        if (boost::is_function<typename boost::remove_pointer<F>::type>::value) {
          strm
              << "Function prototype should be \n" 
              << "\t ReductionType f(icontext_type&, const vertex_type&)\n";
        }
        else {
            strm << "Functor's operator() prototype should be \n"
              << "\t ReductionType operator()(icontext_type&, const vertex_type&) const\n";
        }
        strm << "If you are not intentionally violating the abstraction,"
              << " we recommend fixing your function for safety reasons";
        strm.flush();
        logstream(LOG_WARNING) << strm.str() << std::endl;
      }
    }

    template <typename ReductionType, typename F>
    static void test_edge_mapper_type(std::string key = "") {
      bool test_result = test_function_or_const_functor_2<F,
                                             ReductionType(icontext_type&,
                                                          const edge_type&),
                                             ReductionType,
                                             icontext_type&,
                                             const edge_type&>::value;

      if (!test_result) {
        std::stringstream strm;
        strm << "\n";
        if (key.empty()) {
          strm << "Edge Map Function does not pass strict runtime type checks. \n";
        }
        else {
          strm << "Map Function in Edge Aggregator " << key
              << " does not pass strict runtime type checks. \n";
        }
        if (boost::is_function<typename boost::remove_pointer<F>::type>::value) {
          strm << "Function prototype should be \n"
              << "\t ReductionType f(icontext_type&, const edge_type&)\n";
        }
        else {
            strm << "Functor's operator() prototype should be "
              << "\t ReductionType operator()(icontext_type&, const edge_type&) const\n";
        }
        strm << "If you are not intentionally violating the abstraction,"
            << " we recommend fixing your function for safety reasons";
        logstream(LOG_WARNING) << strm.str() << std::endl;
      }
    }
    
  public:

    
    distributed_aggregator(distributed_control& dc, 
                           graph_type& graph, 
                           icontext_type* context):
                            rmi(dc, this), graph(&graph), 
                            context(context), ncpus(0) { }


    distributed_aggregator(distributed_control& dc, 
                           icontext_type* context):
                            rmi(dc, this), graph(NULL), 
                            context(context), ncpus(0) { }


    /**
     * Resets all aggregation state
     */
    void reset() {
      typename std::map<std::string, imap_reduce_base*>::iterator iter = aggregators.begin();
      while (iter != aggregators.end()) {
        delete iter->second;
        ++iter;
      }
      aggregators.clear();
      aggregate_period.clear();
      async_state.clear();
      start_time = 0;
      schedule.clear();
    }

    /**
     * Associates the aggregator with a new graph object.
     */
    void init(graph_type& new_graph, icontext_type* new_context) {
      reset();
      delete context;
      graph = &new_graph;
      context = new_context;
    }

    /**
     * \copydoc graphlab::iengine::add_vertex_aggregator
     */
    template <typename ReductionType, 
              typename VertexMapperType, 
              typename FinalizerType>
    bool add_vertex_aggregator(const std::string& key,
                               VertexMapperType map_function,
                               FinalizerType finalize_function) {
      if (key.length() == 0) return false;
      if (aggregators.count(key) == 0) {

        if (rmi.procid() == 0) {
          // do a runtime type check
          test_vertex_mapper_type<ReductionType, VertexMapperType>(key);
        }
        
        aggregators[key] = new map_reduce_type<ReductionType,
                                               VertexMapperType,
                                               typename default_map_types<ReductionType>::edge_map_type,
                                               FinalizerType>(map_function, 
                                                             finalize_function);
        return true;
      }
      else {
        // aggregator already exists. fail 
        return false;
      }
    }
    
#if defined(__cplusplus) && __cplusplus >= 201103L
    /**
     * \brief An overload of add_vertex_aggregator for C++11 which does not
     *        require the user to provide the reduction type.
     *
     * This function is available only if the compiler has C++11 support.
     * Specifically, it uses C++11's decltype operation to infer the
     * reduction type, thus eliminating the need for the function
     */
    template <typename VertexMapperType, 
              typename FinalizerType>
    bool add_vertex_aggregator(const std::string& key,
                               VertexMapperType map_function,
                               FinalizerType finalize_function) {
      //typedef decltype(map_function(*context,graph->vertex(0))) ReductionType;
      typedef decltype(map_function(*context, graph->vertex(0))) ReductionType;
      if (key.length() == 0) return false;
      if (aggregators.count(key) == 0) {
        aggregators[key] = new map_reduce_type<ReductionType,
                                               VertexMapperType,
                                               typename default_map_types<ReductionType>::edge_map_type,
                                               FinalizerType>(map_function, 
                                                             finalize_function);
        return true;
      }
      else {
        // aggregator already exists. fail 
        return false;
      }
    }
#endif

    /**
     * \copydoc graphlab::iengine::add_edge_aggregator
     */
    template <typename ReductionType,
              typename EdgeMapperType,
              typename FinalizerType>
    bool add_edge_aggregator(const std::string& key,
                             EdgeMapperType map_function,
                             FinalizerType finalize_function) {
      if (key.length() == 0) return false;
      if (aggregators.count(key) == 0) {
        if (rmi.procid() == 0) {
          // do a runtime type check
          test_edge_mapper_type<ReductionType, EdgeMapperType>(key);
        }
        aggregators[key] = new map_reduce_type<ReductionType, 
                                            typename default_map_types<ReductionType>::vertex_map_type,
                                            EdgeMapperType, 
                                            FinalizerType>(map_function, 
                                                           finalize_function, 
                                                           true);
        return true;
      }
      else {
        // aggregator already exists. fail 
        return false;
      }
    }
    
#if defined(__cplusplus) && __cplusplus >= 201103L
    /**
     * \brief An overload of add_edge_aggregator for C++11 which does not
     *        require the user to provide the reduction type.
     *
     * This function is available only if the compiler has C++11 support.
     * Specifically, it uses C++11's decltype operation to infer the
     * reduction type, thus eliminating the need for the function
     * call to be templatized over the reduction type. 
     */
    template <typename EdgeMapperType,
              typename FinalizerType>
    bool add_edge_aggregator(const std::string& key,
                             EdgeMapperType map_function,
                             FinalizerType finalize_function) {
      // an edge_type is actually hard to get
      typedef decltype(map_function(*context, edge_type(graph->l_vertex(0).in_edges()[0]) )) ReductionType;
      if (key.length() == 0) return false;
      if (aggregators.count(key) == 0) {
        aggregators[key] = new map_reduce_type<ReductionType, 
                                            typename default_map_types<ReductionType>::vertex_map_type,
                                            EdgeMapperType, 
                                            FinalizerType>(map_function, 
                                                           finalize_function, 
                                                           true);
        return true;
      }
      else {
        // aggregator already exists. fail 
        return false;
      }
    }
#endif
    
    /**
     * \copydoc graphlab::iengine::aggregate_now
     */
    bool aggregate_now(const std::string& key) {
      ASSERT_MSG(graph->is_finalized(), "Graph must be finalized");
      if (aggregators.count(key) == 0) {
        ASSERT_MSG(false, "Requested aggregator %s not found", key.c_str());
        return false;
      }
      
      imap_reduce_base* mr = aggregators[key];
      mr->clear_accumulator();
      // ok. now we perform reduction on local data in parallel
#ifdef _OPENMP
#pragma omp parallel
#endif
      {
        imap_reduce_base* localmr = mr->clone_empty();
        if (localmr->is_vertex_map()) {
#ifdef _OPENMP
        #pragma omp for
#endif
          for (int i = 0; i < (int)graph->num_local_vertices(); ++i) {
            local_vertex_type lvertex = graph->l_vertex(i);
            if (lvertex.owner() == rmi.procid()) {
              vertex_type vertex(lvertex);
              localmr->perform_map_vertex(*context, vertex);
            }
          }
        }
        else {
#ifdef _OPENMP
        #pragma omp for
#endif
          for (int i = 0; i < (int)graph->num_local_vertices(); ++i) {
            foreach(local_edge_type e, graph->l_vertex(i).in_edges()) {
              edge_type edge(e);
              localmr->perform_map_edge(*context, edge);
            }
          }
        }
#ifdef _OPENMP
        #pragma omp critical
#endif
        {
          mr->add_accumulator(localmr);
        }
        delete localmr;
      }
      
      std::vector<any> gathervec(rmi.numprocs());
      gathervec[rmi.procid()] = mr->get_accumulator();
      
      rmi.gather(gathervec, 0);
      
      if (rmi.procid() == 0) {
        // machine 0 aggregates the accumulators
        // sums them together and broadcasts it
        for (procid_t i = 1; i < rmi.numprocs(); ++i) {
          mr->add_accumulator_any(gathervec[i]);
        }
        any val = mr->get_accumulator();
        rmi.broadcast(val, true);
      }
      else {
        // all other machines wait for the broadcast value
        any val;
        rmi.broadcast(val, false);
        mr->set_accumulator_any(val);
      }
      mr->finalize(*context);
      mr->clear_accumulator();
      gathervec.clear();
      return true;
    }
    
    
    /**
     * \copydoc graphlab::iengine::aggregate_periodic
     */
    bool aggregate_periodic(const std::string& key, float seconds) {
      rmi.barrier();
      if (seconds < 0) return false;
      if (aggregators.count(key) == 0) return false;
      else aggregate_period[key] = seconds;
      return true;
    }
    
    /**
     * Performs aggregation on all keys registered with a period.
     * May be used on engine start() to ensure all periodic 
     * aggregators are executed before engine execution.
     */
    void aggregate_all_periodic() {
      typename std::map<std::string, float>::iterator iter =
        aggregate_period.begin();
      while (iter != aggregate_period.end()) { 
        aggregate_now(iter->first);
        ++iter;
      }
    }
    
    
    /**
     * Must be called on engine start. Initializes the internal scheduler.
     * Must be called on all machines simultaneously.
     * ncpus is really only important for the asynchronous implementation.
     * It must be equal to the number of engine threads.
     *
     * \param [in] cpus Number of engine threads used. This is only necessary
     *                  if the asynchronous form is used.
     */
    void start(size_t ncpus = 0) {
      rmi.barrier();
      schedule.clear();
      start_time = timer::approx_time_seconds();
      typename std::map<std::string, float>::iterator iter =
                                                    aggregate_period.begin();
      while (iter != aggregate_period.end()) {
        // schedule is a max heap. To treat it like a min heap
        // I need to insert negative keys
        schedule.push(iter->first, -iter->second);
        ++iter;
      }
      this->ncpus = ncpus;

      // now initialize the asyncronous reduction states
      if(ncpus > 0) {
        iter = aggregate_period.begin();
        while (iter != aggregate_period.end()) {
          async_state[iter->first].local_count_down = (int)ncpus;
          async_state[iter->first].distributed_count_down =
                                                        (int)rmi.numprocs();
          
          async_state[iter->first].per_thread_aggregation.resize(ncpus);
          for (size_t i = 0; i < ncpus; ++i) {
            async_state[iter->first].per_thread_aggregation[i] =
                                    aggregators[iter->first]->clone_empty();
          }
          async_state[iter->first].root_reducer =
                                      aggregators[iter->first]->clone_empty();
          ++iter;
        }
      }
    }
    
    
    /**
     * If asynchronous aggregation is desired, this function is
     * to be called periodically on each machine. This polls the schedule to
     * check if there is an aggregator which needs to be activated. If there
     * is an aggregator to be started, this function will return a non empty
     * string. This function is thread reentrant and each activated aggregator
     * will only return a non empty string call to one call to
     * tick_asynchronous() on each machine.
     * 
     * If an empty is returned, the asynchronous engine
     * must ensure that all threads (ncpus per machine) must eventually
     * call tick_asynchronous_compute(cpuid, key) where key is the return string.
     */ 
    std::string tick_asynchronous() {
      // if we fail to acquire the lock, go ahead
      if (!schedule_lock.try_lock()) return "";
      
      // see if there is a key to run
      float curtime = timer::approx_time_seconds() - start_time;
      std::string key;
      bool has_entry = false;
      if (!schedule.empty() && -schedule.top().second <= curtime) {
        key = schedule.top().first;
        has_entry = true;
        schedule.pop();
      }
      schedule_lock.unlock();

      // no key to run. return false
      if (has_entry == false) return "";
      else return key;
      // ok. we have a key to run, construct the local reducers
    }

    
    /**
     * Once tick_asynchronous() returns a key, all threads in the engine
     * should call tick_asynchronous_compute() with a matching key.
     * This function will perform the computation for the key in question
     * and send the accumulated result back to machine 0 when done
     */
    void tick_asynchronous_compute(size_t cpuid, const std::string& key) {
      // acquire and check the async_aggregator_state
      typename std::map<std::string, async_aggregator_state>::iterator iter =
                                                        async_state.find(key);
      ASSERT_MSG(iter != async_state.end(), "Key %s not found", key.c_str());
      ASSERT_GT(iter->second.per_thread_aggregation.size(), cpuid);
      
      imap_reduce_base* localmr = iter->second.per_thread_aggregation[cpuid];
      // perform the reduction using the local mr
      if (localmr->is_vertex_map()) {
        for (int i = cpuid;i < (int)graph->num_local_vertices(); i+=ncpus) {
          local_vertex_type lvertex = graph->l_vertex(i);
          if (lvertex.owner() == rmi.procid()) {
            vertex_type vertex(lvertex);
            localmr->perform_map_vertex(*context, vertex);
          }
        }
      } else {
        for (int i = cpuid;i < (int)graph->num_local_vertices(); i+=ncpus) {
          foreach(local_edge_type e, graph->l_vertex(i).in_edges()) {
            edge_type edge(e);
            localmr->perform_map_edge(*context, edge);
          }
        }
      }
      iter->second.root_reducer->add_accumulator(localmr);
      int countdown_val = iter->second.local_count_down.dec();

      ASSERT_LT(countdown_val, ncpus);
      ASSERT_GE(countdown_val, 0);
      if (countdown_val == 0) {
        // reset the async_state to pristine condition.
        // - clear all thread reducers since we got all we need from them
        // - clear all the local root reducer except for machine 0 (and after
        //   we read the accumulator from them.
        // - reset the counters
        for (size_t i = 0;
             i < iter->second.per_thread_aggregation.size(); ++i) {
          iter->second.per_thread_aggregation[i]->clear_accumulator();
        }
        iter->second.local_count_down = ncpus;
        
        if (rmi.procid() != 0) {
          // ok we need to signal back to the the root to perform finalization
          // read the accumulator
          any acc = iter->second.root_reducer->get_accumulator();
          iter->second.root_reducer->clear_accumulator();
          rmi.remote_call(0, &distributed_aggregator::rpc_key_merge,
                          key, acc);
        }
        else {
          decrement_distributed_counter(key);
        }
      }
    }

    /**
     * RPC Call called by other machines with their accumulator for the key.
     * This function will merge the accumulator and perform finalization
     * when all accumulators are received
     */
    void rpc_key_merge(const std::string& key, any& acc) {
      // acquire and check the async_aggregator_state 
      typename std::map<std::string, async_aggregator_state>::iterator iter =
                                                      async_state.find(key);
      ASSERT_MSG(iter != async_state.end(), "Key %s not found", key.c_str());
      iter->second.root_reducer->add_accumulator_any(acc);
      decrement_distributed_counter(key);
    }

    /**
     * Called whenever one machine finishes all of its local accumulation.
     * When the counter determines that all machine's accumulators have been
     * received, this function performs finalization and prepares and
     * broadcasts the next scheduled time for the key.
     */
    void decrement_distributed_counter(const std::string& key) {
      // must be master machine
      ASSERT_EQ(rmi.procid(), 0);
      // acquire and check the async_aggregator_state 
      typename std::map<std::string, async_aggregator_state>::iterator iter =
                                                      async_state.find(key);
      ASSERT_MSG(iter != async_state.end(), "Key %s not found", key.c_str());
      int countdown_val = iter->second.distributed_count_down.dec();
      logstream(LOG_INFO) << "Distributed Aggregation of " << key << ". "
                          << countdown_val << " remaining." << std::endl;

      ASSERT_LE(countdown_val, rmi.numprocs());
      ASSERT_GE(countdown_val, 0);
      if (countdown_val == 0) {
        logstream(LOG_INFO) << "Aggregate completion of " << key << std::endl;
        any acc_val = iter->second.root_reducer->get_accumulator();
        // set distributed count down again for the second phase:
        // waiting for everyone to finish finalization
        iter->second.distributed_count_down = rmi.numprocs();
        for (procid_t i = 1;i < rmi.numprocs(); ++i) {
          rmi.remote_call(i, &distributed_aggregator::rpc_perform_finalize,
                            key, acc_val);
        }
        iter->second.root_reducer->finalize(*context);
        iter->second.root_reducer->clear_accumulator();
        decrement_finalize_counter(key);
      }
    }

    /**
     * Called from the root machine to all machines to perform finalization
     * on the key
     */
    void rpc_perform_finalize(const std::string& key, any& acc_val) {
      ASSERT_NE(rmi.procid(), 0);
      typename std::map<std::string, async_aggregator_state>::iterator iter =
                                                  async_state.find(key);
      ASSERT_MSG(iter != async_state.end(), "Key %s not found", key.c_str());
      
      iter->second.root_reducer->set_accumulator_any(acc_val);
      iter->second.root_reducer->finalize(*context);
      iter->second.root_reducer->clear_accumulator();
      // reply to the root machine
      rmi.remote_call(0, &distributed_aggregator::decrement_finalize_counter,
                      key);
    }


    void decrement_finalize_counter(const std::string& key) {
      typename std::map<std::string, async_aggregator_state>::iterator iter =
                                                      async_state.find(key);
      ASSERT_MSG(iter != async_state.end(), "Key %s not found", key.c_str());
      int countdown_val = iter->second.distributed_count_down.dec();
      if (countdown_val == 0) {
        // done! all finalization is complete.
        // reset the counter
        iter->second.distributed_count_down = rmi.numprocs();
        // when is the next time we start. 
        // time is as an offset to start_time
        float next_time = timer::approx_time_seconds() + 
                          aggregate_period[key] - start_time;
        logstream(LOG_INFO) << rmi.procid() << "Reschedule of " << key
                          << " at " << next_time << std::endl;
        rpc_schedule_key(key, next_time);
        for (procid_t i = 1;i < rmi.numprocs(); ++i) {
          rmi.remote_call(i, &distributed_aggregator::rpc_schedule_key,
                            key, next_time);
        }
      }
    }

    /**
     * Called to schedule the next trigger time for the key
     */
    void rpc_schedule_key(const std::string& key, float next_time) {
      schedule_lock.lock();
      schedule.push(key, -next_time);
      schedule_lock.unlock();
    }

    
    /**
     * If synchronous aggregation is desired, this function is
     * To be called simultaneously by one thread on each machine. 
     * This polls the schedule to see if there
     * is an aggregator which needs to be activated. If there is an aggregator 
     * to be started, this function will perform aggregation.
     */ 
    void tick_synchronous() {
      // if timer has exceeded our top key
      float curtime = timer::approx_time_seconds() - start_time;
      rmi.broadcast(curtime, rmi.procid() == 0);
      // note that we do not call approx_time_seconds everytime
      // this ensures that each key will only be run at most once.
      // each time tick_synchronous is called.
      std::vector<std::pair<std::string, float> > next_schedule;
      while(!schedule.empty() && -schedule.top().second <= curtime) {
        std::string key = schedule.top().first;
        aggregate_now(key);
        schedule.pop();
        // when is the next time we start. 
        // time is as an offset to start_time
        float next_time = (timer::approx_time_seconds() + 
                           aggregate_period[key] - start_time);
        rmi.broadcast(next_time, rmi.procid() == 0);
        next_schedule.push_back(std::make_pair(key, -next_time));
      }

      for (size_t i = 0;i < next_schedule.size(); ++i) {
        schedule.push(next_schedule[i].first, next_schedule[i].second);
      }
    }

    /**
     * Must be called on engine stop. Clears the internal scheduler
     * And resets all incomplete states.
     */
    void stop() {
      schedule.clear();
      // clear the aggregators
      {
        typename std::map<std::string, imap_reduce_base*>::iterator iter =
                                                          aggregators.begin();
        while (iter != aggregators.end()) {
          iter->second->clear_accumulator();
          ++iter;
        }
      }
      // clear the asynchronous state
      {
        typename std::map<std::string, async_aggregator_state>::iterator
                                                  iter = async_state.begin();
        while (iter != async_state.end()) {
          delete iter->second.root_reducer;
          for (size_t i = 0;
               i < iter->second.per_thread_aggregation.size();
               ++i) {
            delete iter->second.per_thread_aggregation[i];
          }
          iter->second.per_thread_aggregation.clear();
          ++iter;
        }
        async_state.clear();
      }
    }


    std::set<std::string> get_all_periodic_keys() const {
      typename std::map<std::string, float>::const_iterator iter =
                                                    aggregate_period.begin();
      std::set<std::string> ret;
      while (iter != aggregate_period.end()) {
        ret.insert(iter->first);
        ++iter;
      }
      return ret;
    }
    
    
    
    
    template <typename ResultType, typename MapFunctionType>
    ResultType map_reduce_vertices(MapFunctionType mapfunction) {
      ASSERT_MSG(graph->is_finalized(), "Graph must be finalized");

      if (rmi.procid() == 0) {
        // do a runtime type check
        test_vertex_mapper_type<ResultType, MapFunctionType>();
      }
      
      rmi.barrier();
      bool global_result_set = false;
      ResultType global_result = ResultType();
#ifdef _OPENMP
#pragma omp parallel
#endif
      {
        bool result_set = false;
        ResultType result = ResultType();
#ifdef _OPENMP
        #pragma omp for
#endif
        for (int i = 0; i < (int)graph->num_local_vertices(); ++i) {
          if (graph->l_vertex(i).owner() == rmi.procid()) {
            if (!result_set) {
              vertex_type vtx(graph->l_vertex(i));
              result = mapfunction(*context, vtx);
              result_set = true;
            }
            else if (result_set){
              vertex_type vtx(graph->l_vertex(i));
              result += mapfunction(*context, vtx);
            }
          }
        }
#ifdef _OPENMP
        #pragma omp critical
#endif
        {
          if (result_set) {
            if (!global_result_set) {
              global_result = result;
              global_result_set = true;
            }
            else {
              global_result += result;
            }
          }
        }
      }
      conditional_addition_wrapper<ResultType> wrapper(global_result, global_result_set);
      rmi.all_reduce(wrapper);
      return wrapper.value;
    }


  
    template <typename ResultType, typename MapFunctionType>
    ResultType map_reduce_edges(MapFunctionType mapfunction) {
      ASSERT_MSG(graph->is_finalized(), "Graph must be finalized");
      
      if (rmi.procid() == 0) {
        // do a runtime type check
        test_edge_mapper_type<ResultType, MapFunctionType>();
      }
      
      rmi.barrier();
      bool global_result_set = false;
      ResultType global_result = ResultType();
#ifdef _OPENMP
#pragma omp parallel
#endif
      {
        bool result_set = false;
        ResultType result = ResultType();
#ifdef _OPENMP
        #pragma omp for
#endif
        for (int i = 0; i < (int)graph->num_local_vertices(); ++i) {
          foreach(const local_edge_type& e, graph->l_vertex(i).in_edges()) {
            if (!result_set) {
              edge_type edge(e);
              result = mapfunction(*context, edge);
              result_set = true;
            }
            else if (result_set){
              edge_type edge(e);
              result += mapfunction(*context, edge);
            }
          }
        }
#ifdef _OPENMP
        #pragma omp critical
#endif
        {
          if (result_set) {
            if (!global_result_set) {
              global_result = result;
              global_result_set = true;
            }
            else {
              global_result += result;
            }
          }
        }
      }

      conditional_addition_wrapper<ResultType> wrapper(global_result, global_result_set);
      rmi.all_reduce(wrapper);
      return wrapper.value;
    }

    template <typename TransformType>
    void transform_vertices(TransformType transform_functor) {
      ASSERT_MSG(graph->is_finalized(), "Graph must be finalized");
      rmi.barrier();
#ifdef _OPENMP
      #pragma omp parallel for
#endif
      for (int i = 0; i < (int)graph->num_local_vertices(); ++i) {
        if (graph->l_vertex(i).owner() == rmi.procid()) {
          vertex_type vtx(graph->l_vertex(i));
          transform_functor(*context, vtx);
        }
      }
      rmi.barrier();
      graph->synchronize();
    }


    template <typename TransformType>
    void transform_edges(TransformType transform_functor) {
      ASSERT_MSG(graph->is_finalized(), "Graph must be finalized");
      rmi.barrier();
#ifdef _OPENMP
      #pragma omp parallel for
#endif
      for (int i = 0; i < (int)graph->num_local_vertices(); ++i) {
        foreach(const local_edge_type& e, graph->l_vertex(i).in_edges()) {
          edge_type edge(e);
          transform_functor(*context, edge);
        }
      }
      rmi.barrier();
    }
    
    
    
    
    
    ~distributed_aggregator() {
      reset();
      delete context;
    }
  }; 


}; // end of graphlab namespace
#include <graphlab/macros_undef.hpp>

#endif
