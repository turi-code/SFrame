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
 * @file org_graphlab_Core.cpp
 *
 * Contains the JNI interface for org.graphlab.Core. In general, applications
 * will keep their graphs in the Java layer and access the engine through the
 * JNI. This wrapper provides a proxy graph for the engine to manipulate and
 * forwards update calls to the Java layer. To learn how to use this interface,
 * refer to the org.graphlab.Core class and to the examples.
 *
 * @author Jiunn Haur Lim <jiunnhal@cmu.edu>
 */

#include <wordexp.h>
#include "org_graphlab_Core.hpp"
#include "org_graphlab_Updater.hpp"
#include "org_graphlab_Aggregator.hpp"

using namespace graphlab;

//---------------------------------------------------------------
// jni_core static members
//---------------------------------------------------------------

template<typename G, typename U>
JavaVM* jni_core<G, U>::mjvm = NULL;

template<typename G, typename U>
const size_t jni_core<G, U>::ENV_ID = 1;

#ifdef __cplusplus
extern "C" {
#endif

//---------------------------------------------------------------
// static helper functions
//---------------------------------------------------------------

  static jlong createCore (JNIEnv *env, jobject obj, int argc, char **argv){
  
    // configure log level
    global_logger().set_log_level(LOG_DEBUG);
    global_logger().set_log_to_console(false);

    // set jvm, if we don't have it already
    if (NULL == proxy_updater::core::get_jvm()){
      JavaVM* jvm = NULL;
      env->GetJavaVM(&jvm);
      proxy_updater::core::set_jvm(jvm);
    }
    
    // store env for this thread
    thread::get_local(proxy_updater::core::ENV_ID) = env;
    
    // allocate and configure core
    proxy_updater::core *jni_core = new proxy_updater::core(env, obj);
    if (NULL != argv){
      (*jni_core)().parse_options(argc, argv);
    }
    
    // return address of jni_core
    return (long) jni_core;
  
  }
  
//---------------------------------------------------------------
// JNI functions
//---------------------------------------------------------------

  JNIEXPORT jlong JNICALL
  Java_org_graphlab_Core_createCore__
  (JNIEnv *env, jobject obj){
    return createCore(env, obj, 0, NULL);
  }
  
  JNIEXPORT jlong JNICALL
  Java_org_graphlab_Core_createCore__Ljava_lang_String_2
  (JNIEnv *env, jobject obj, jstring command_line_args){
  
    // convert jstring to c string
    const char *cstr = NULL;
    cstr = env->GetStringUTFChars(command_line_args, NULL);
    if (NULL == cstr) {
       return 0; /* OutOfMemoryError already thrown */
    }
    
    // prepend with dummy name
    char buffer[1024];
    snprintf(buffer, 1024, "x %s", cstr);
    env->ReleaseStringUTFChars(command_line_args, cstr);
    
    // split string
    wordexp_t we_result;
    if (0 != wordexp(buffer, &we_result, 0)) return 0;
   
    // create core
    jlong ptr = createCore(env, obj, we_result.we_wordc, we_result.we_wordv);
   
    // cleanup
    wordfree(&we_result);
    return ptr;
  
  }
  
  JNIEXPORT void JNICALL
  Java_org_graphlab_Core_destroyCore
  (JNIEnv *env, jobject obj, jlong ptr){
    
    if (NULL == env || 0 == ptr){
      proxy_updater::core::throw_exception(
        env,
        "java/lang/IllegalArgumentException",
        "ptr must not be null.");
      return;
    }
    
    proxy_updater::core *jni_core = (proxy_updater::core *) ptr;
    
    // cleanup core
    delete jni_core;
    
  }

  JNIEXPORT void JNICALL
  Java_org_graphlab_Core_resizeGraph
  (JNIEnv *env, jobject obj, jlong ptr, jint count){
    
    if (NULL == env || 0 == ptr){
      proxy_updater::core::throw_exception(
        env,
        "java/lang/IllegalArgumentException",
        "ptr must not be null.");
      return;
    }
    
    proxy_updater::core *jni_core = (proxy_updater::core *) ptr;
    (*jni_core)().graph().resize(count);
    
  }
  
  JNIEXPORT void JNICALL
  Java_org_graphlab_Core_addVertex
  (JNIEnv *env, jobject obj, jlong ptr,
   jobject app_vertex, jint vertex_id){
  
    if (NULL == env || 0 == ptr){
      proxy_updater::core::throw_exception(
        env,
        "java/lang/IllegalArgumentException",
        "ptr must not be null.");
      return;
    }
    
    proxy_updater::core *jni_core = (proxy_updater::core *) ptr;
    
    // add to graph
    (*jni_core)().graph()
      .add_vertex(vertex_id, proxy_vertex(env, app_vertex));
       
  }
  
  JNIEXPORT void JNICALL
  Java_org_graphlab_Core_addEdge
  (JNIEnv *env, jobject obj, jlong ptr,
  jint source, jint target, jobject app_edge){
  
    if (NULL == env || 0 == ptr){
      proxy_updater::core::throw_exception(
        env,
        "java/lang/IllegalArgumentException",
        "ptr must not be null.");
        return;
    }
    
    proxy_updater::core *jni_core = (proxy_updater::core *) ptr;
    
    // add to graph
    (*jni_core)().graph().add_edge(source, target, proxy_edge(env, app_edge));
  
  }
  
  JNIEXPORT jdouble JNICALL
  Java_org_graphlab_Core_start
  (JNIEnv *env, jobject obj, jlong ptr){
    
    if (NULL == env || 0 == ptr){
      proxy_updater::core::throw_exception(
        env,
        "java/lang/IllegalArgumentException",
        "ptr must not be null.");
        return 0;
    }
    
    proxy_updater::core *jni_core = (proxy_updater::core *) ptr;
    (*jni_core)().engine().get_options().print();
    
    double runtime = (*jni_core)().start();
    return runtime;
    
  }
  
  JNIEXPORT jlong JNICALL
  Java_org_graphlab_Core_lastUpdateCount
  (JNIEnv *env, jobject obj, jlong ptr){
  
    if (NULL == env || 0 == ptr){
      proxy_updater::core::throw_exception(
        env,
        "java/lang/IllegalArgumentException",
        "ptr must not be null.");
        return 0;
    }
  
    proxy_updater::core *jni_core = (proxy_updater::core *) ptr;
    return (*jni_core)().engine().last_update_count();
    
  }

  JNIEXPORT void JNICALL
  Java_org_graphlab_Core_addGlobalConst
  (JNIEnv *env, jobject obj, jlong ptr, jstring key, jobject to_store){
  
    if (NULL == env || 0 == ptr){
      proxy_updater::core::throw_exception(
        env,
        "java/lang/IllegalArgumentException",
        "ptr must not be null.");
        return;
    }
    
    proxy_updater::core *jni_core = (proxy_updater::core *) ptr;
    
    // convert jstring to c string
    const char * key_str = env->GetStringUTFChars(key, NULL);
    
    (*jni_core)().add_global_const(std::string(key_str), java_any(env, to_store));
    
    // free memory
    env->ReleaseStringUTFChars(key, key_str);
    
    return;
  
  }
  
  JNIEXPORT void JNICALL
  Java_org_graphlab_Core_addGlobal
  (JNIEnv *env, jobject obj, jlong ptr, jstring key, jobject to_store){
  
    if (NULL == env || 0 == ptr){
      proxy_updater::core::throw_exception(
        env,
        "java/lang/IllegalArgumentException",
        "ptr must not be null.");
        return;
    }
    
    proxy_updater::core *jni_core = (proxy_updater::core *) ptr;
    
    // convert jstring to c string
    const char * key_str = env->GetStringUTFChars(key, NULL);
    
    java_any a = java_any(env, to_store);
    (*jni_core)().add_global(std::string(key_str), a);
    
    // free memory
    env->ReleaseStringUTFChars(key, key_str);
    
    return;
  
  }
  
  JNIEXPORT void JNICALL
  Java_org_graphlab_Core_setGlobal
  (JNIEnv *env, jobject obj, jlong ptr, jstring key, jobject to_store){
  
    if (NULL == env || 0 == ptr){
      proxy_updater::core::throw_exception(
        env,
        "java/lang/IllegalArgumentException",
        "ptr must not be null.");
        return;
    }
    
    proxy_updater::core *jni_core = (proxy_updater::core *) ptr;
    
    // convert jstring to c string
    const char * key_str = env->GetStringUTFChars(key, NULL);
    
    java_any a = java_any(env, to_store);
    (*jni_core)().set_global(std::string(key_str), a);
    
    // free memory
    env->ReleaseStringUTFChars(key, key_str);
    
    return;
  
  }
  

  JNIEXPORT jobject JNICALL
  Java_org_graphlab_Core_getGlobal
  (JNIEnv *env, jobject obj, jlong ptr, jstring key){
  
    if (NULL == env || 0 == ptr){
      proxy_updater::core::throw_exception(
        env,
        "java/lang/IllegalArgumentException",
        "ptr must not be null.");
        return NULL;
    }
    
     proxy_updater::core *jni_core = (proxy_updater::core *) ptr;
    
    // convert jstring to c string
    const char * key_str = env->GetStringUTFChars(key, NULL);
    
    java_any stored = (*jni_core)().get_global<java_any>(std::string(key_str));
    
    // free memory
    env->ReleaseStringUTFChars(key, key_str);
    
    return env->NewLocalRef(stored.obj());
  
  }

  JNIEXPORT void JNICALL
  Java_org_graphlab_Core_setNCpus
  (JNIEnv * env, jobject obj, jlong ptr, jlong ncpus) {
  
    if (NULL == env || 0 == ptr){
      proxy_updater::core::throw_exception(
        env,
        "java/lang/IllegalArgumentException",
        "ptr must not be null.");
        return;
    }
  
    proxy_updater::core *jni_core = (proxy_updater::core *) ptr;
    (*jni_core)().set_ncpus(ncpus);
    
  }

  JNIEXPORT void JNICALL
  Java_org_graphlab_Core_setSchedulerType
  (JNIEnv * env, jobject obj, jlong ptr, jstring scheduler_str) {
  
    if (NULL == env || 0 == ptr){
      proxy_updater::core::throw_exception(
        env,
        "java/lang/IllegalArgumentException",
        "ptr must not be null.");
        return;
    }
  
    const char *str = env->GetStringUTFChars(scheduler_str, NULL);
    if (NULL == str) return;  // OutOfMemoryError already thrown
    
    proxy_updater::core *jni_core = (proxy_updater::core *) ptr;
    (*jni_core)().set_scheduler_type(std::string(str));
    env->ReleaseStringUTFChars(scheduler_str, str);
    
  }

  JNIEXPORT void JNICALL
  Java_org_graphlab_Core_setScopeType
  (JNIEnv * env, jobject obj, jlong ptr, jstring scope_str) {
  
    if (NULL == env || 0 == ptr){
      proxy_updater::core::throw_exception(
        env,
        "java/lang/IllegalArgumentException",
        "ptr must not be null.");
        return;
    }
  
    const char *str = env->GetStringUTFChars(scope_str, NULL);
    if (NULL == str) return;  // OutOfMemoryError already thrown
    
    proxy_updater::core *jni_core = (proxy_updater::core *) ptr;
    (*jni_core)().set_scope_type(std::string(str));
    env->ReleaseStringUTFChars(scope_str, str);
    
  }
  
  JNIEXPORT void JNICALL
  Java_org_graphlab_Core_schedule
  (JNIEnv * env, jobject obj,
  jlong core_ptr, jobject updater, jint vertex_id){
  
    if (NULL == env || 0 == core_ptr){
      proxy_updater::core::throw_exception(
        env,
        "java/lang/IllegalArgumentException",
        "core_ptr must not be null.");
        return;
    }

    // get objects from pointers
    proxy_updater::core *jni_core = (proxy_updater::core *) core_ptr;

    // schedule vertex
    (*jni_core)().schedule(vertex_id, proxy_updater(env, updater));
    
  }
  
  JNIEXPORT void JNICALL 
  Java_org_graphlab_Core_scheduleAll
  (JNIEnv * env, jobject obj,
  jlong core_ptr, jobject updater) {

    if (NULL == env || 0 == core_ptr){
    proxy_updater::core::throw_exception(
        env,
        "java/lang/IllegalArgumentException",
        "core_ptr and updater_ptr must not be null.");
        return;
    }

    // get objects from pointers
    proxy_updater::core *jni_core = (proxy_updater::core *) core_ptr;

    // schedule vertex
    (*jni_core)().schedule_all(proxy_updater(env, updater));

  }
  
  JNIEXPORT void JNICALL
  Java_org_graphlab_Core_addAggregator
  (JNIEnv * env, jobject obj,
  jlong core_ptr, jstring key, jobject aggregator,
  jlong frequency){
    
    if (NULL == env || 0 == core_ptr){
    proxy_updater::core::throw_exception(
        env,
        "java/lang/IllegalArgumentException",
        "core_ptr and updater_ptr must not be null.");
        return;
    }

    // get objects from pointers
    proxy_updater::core *jni_core = (proxy_updater::core *) core_ptr;

    // add aggregator
    const char * key_str = env->GetStringUTFChars(key, NULL);
    (*jni_core)().add_aggregator(std::string(key_str),
                                proxy_aggregator(env, aggregator),
                                frequency);
    env->ReleaseStringUTFChars(key, key_str);
    
  }
  
  JNIEXPORT void JNICALL
  Java_org_graphlab_Core_aggregateNow
  (JNIEnv * env, jobject obj,
  jlong core_ptr, jstring key){
  
    if (NULL == env || 0 == core_ptr){
    proxy_updater::core::throw_exception(
        env,
        "java/lang/IllegalArgumentException",
        "core_ptr and updater_ptr must not be null.");
        return;
    }

    // get objects from pointers
    proxy_updater::core *jni_core = (proxy_updater::core *) core_ptr;

    // add aggregator
    const char * key_str = env->GetStringUTFChars(key, NULL);
    (*jni_core)().aggregate_now(std::string(key_str));
    env->ReleaseStringUTFChars(key, key_str);
  
  }

#ifdef __cplusplus
}
#endif
