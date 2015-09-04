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

/**
 * @file org_graphlab_Aggregator.hpp
 * @author Jiunn Haur Lim <jiunnhal@cmu.edu>
 */

#ifndef ORG_GRAPHLAB_AGGREGATOR_HPP
#define ORG_GRAPHLAB_AGGREGATOR_HPP

#include <graphlab.hpp>
#include "java_any.hpp"
#include "org_graphlab_Core.hpp"
#include "org_graphlab_Updater.hpp"

#include "org_graphlab_Aggregator.h"

namespace graphlab {
  
  /**
   * Proxy aggregator.
   * Mirrors and forwards calls to the corresponding Java aggregator.
   * The constructor creates a new reference to the Java object (so that it
   * doesn't get garbage collected.) The destructor will delete the reference
   * to allow the corresponding Java object to be garbaged collected. The copy
   * constructor clones the Java object.
   */
  class proxy_aggregator : 
    public iaggregator<proxy_graph, proxy_updater, proxy_aggregator>,
    public java_any {
    
  private:
  
    typedef proxy_updater::core core;
    
  public:
  
    /**
     * Method ID of org.graphlab.Aggregator#exec.
     */
    static jmethodID java_exec;
    
    /**
     * Method ID of org.graphlab.Aggregator#add.
     */
    static jmethodID java_add;
    
    /**
     * Method ID of org.graphlab.Aggregator#finalize.
     */
    static jmethodID java_finalize;
    
    /**
     * Method ID of org.graphlab.Aggregator#clone
     */
    static jmethodID java_clone;
    
    /**
     * Constructor for proxy aggregator.
     * Initializes this object with the associated Java org.graphlab.Updater
     * object.
     * @param[in] env           JNI environment - used to create a new reference
     *                          to javaUpdater.
     * @param[in] java_aggregator  Java org.graphlab.Aggregator object. This constructor
     *                          will create a new reference to the object to prevent
     *                          garbage collection.
     */
    proxy_aggregator(JNIEnv *env, jobject &java_aggregator);
    
    /** The default constructor does nothing */
    proxy_aggregator();
    
    /**
     * Copy constructor for proxy_aggregator.
     * If \c other has a \c mobj, creates a new reference to it.
     */
    proxy_aggregator(const proxy_aggregator& other);
    
    /**
     * Copy assignment operator for proxy_aggregator.
     * If \c other has a \c mobj, creates a new reference to it.
     */
    proxy_aggregator &operator=(const proxy_aggregator &other);
    
    /**
     * Deletes the reference to the Java object so that it may be garbage
     * collected.
     */
    ~proxy_aggregator();
    
    void operator()(icontext_type& context);
    
    void operator+=(const proxy_aggregator& other);
    
    void finalize(iglobal_context& context);
    
  };

};

#endif
