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
 * @file org_graphlab_Core.hpp
 * \c javah will generate \c org_graphlab_Core.h from the native methods
 * defined in \c org.graphlab.Context (and so will overwrite the file every time).
 * Define any additional classes/structs/typedefs in this hpp file.
 * @author Jiunn Haur Lim <jiunnhal@cmu.edu>
 */

#ifndef ORG_GRAPHLAB_CORE_HPP
#define ORG_GRAPHLAB_CORE_HPP

#include <execinfo.h>
#include <graphlab.hpp>

#include "java_any.hpp"
#include "org_graphlab_Core.h"

namespace graphlab {

  /**
   * Wrapper for graphlab::core.
   * Contains the core, a reference to the Java core object (so that it
   * doesn't get garbage collected), and other utility functions for dealing
   * with the JVM.
   */
  template <typename Graph, typename UpdateFunctor>
  class jni_core : public java_any {
    
  public:
  
    /** ID of pointer to JNI environment in thread local store */
    static const size_t ENV_ID;
    
  private:
  
    typedef core<Graph, UpdateFunctor> core_type;
    
    /** graphlab::core object - the soul that this body wraps around */
    core_type *mcore;
    
    /** Java virtual machine reference - set only once for each process */
    static JavaVM *mjvm;
    
  public:

    /**
     * Creates a new graphlab core and a new reference to the associated
     * Java org.graphlab.Core object (so that it doesn't get garbage collected.)
     * @param[in] env   JNI environment, which will be used to create the
     *                  reference to the Java object.
     * @param[in] obj   associated org.graphlab.Core object.
     */
    jni_core (JNIEnv *env, jobject &obj) : java_any (env, obj) {
      this->mcore = new core_type();
    }
    
    /**
     * Gets the real graphlab core that this method wraps around
     * @return graphlab::core
     */
    core_type &operator()(){
      return *mcore;
    }
    
    /**
     * Deallocates the graphlab core. Parent constructor will delete jobject
     * reference.
     */
    ~jni_core(){
      delete mcore;
    }
    
    /**
     * Saves a reference to the Java Virtual Machine.
     * @param[in] jvm   pointer to the Java Virtual Machine
     */
    static void set_jvm (JavaVM *jvm){
      mjvm = jvm;
    }
    
    /**
     * Gets a reference to the Java Virtual Machine.
     * @return pointer to the Java Virtual Machine
     */
    static JavaVM *get_jvm (){
      return mjvm;
    }
    
    /**
     * Detaches the current thread from the JVM.
     * If a pointer to the JNI environment cannot be found in the thread-local
     * store, that means that this thread has already been detached, and the
     * function will return immediately. Otherwise, the thread is detached and
     * the pointer to the JNI environment is removed from the thread-local
     * store.
     */
    static void detach_from_jvm() {
      
      // if the current thread is still attached, detach it
      if (thread::contains(ENV_ID)) {
        int res = mjvm->DetachCurrentThread();
        assert(res >= 0);
        thread::get_local(ENV_ID) = NULL;
      }
      
    }
    
    static void dump_backtrace(int sig){
    
      void *array[10];
      size_t size;
      
      // get void*'s for all entries on the stack
      size = backtrace(array, 10);
      
      // print out all the frames to stderr
      backtrace_symbols_fd(array, size, 2);
      
    }
    
    /** Convenience method for throwing Java exceptions */
    static void throw_exception(JNIEnv* env,
                                const char *exception,
                                const char *message){
      jclass exc = env->FindClass(exception);
      if (NULL == exc) return;
      env->ThrowNew(exc, message);
    }
    
    /**
     * Retrieves the JNI environment for the current thread.
     * If a pointer to the JNI environment can be found in the thread-local
     * store, returns immediately; otherwise, that means that the current
     * thread has not been attached to the JVM yet. In that case, this 
     * function will attach the current thread to the JVM and save the
     * associated JNI environment to the thread-local storage.
     * @return JNI environment associated with the current thread.
     */
    static JNIEnv *get_jni_env (){
    
      JNIEnv *jenv = NULL;
    
      // if current thread is not already on the JVM, attach it    
      if (!thread::contains(ENV_ID)) {
      
        int res = mjvm->AttachCurrentThread((void **)&jenv, NULL);
        assert(res >= 0);
        
        // store JNI environment in thread-local storage
        thread::get_local(ENV_ID) = jenv;
        thread::set_thread_destroy_callback(detach_from_jvm);
          
      }
      
      // return the environment associated with the current thread
      return thread::get_local(ENV_ID).as<JNIEnv *>();
      
    }
    
  };

}

#endif
