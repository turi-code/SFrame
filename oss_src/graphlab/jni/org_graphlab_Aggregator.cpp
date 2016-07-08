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

#include "org_graphlab_Aggregator.hpp"

using namespace graphlab;

//---------------------------------------------------------------
// proxy_aggregator static members
//---------------------------------------------------------------

jmethodID proxy_aggregator::java_exec     = 0;
jmethodID proxy_aggregator::java_add      = 0;
jmethodID proxy_aggregator::java_finalize = 0;
jmethodID proxy_aggregator::java_clone    = 0;

JNIEXPORT void JNICALL
  Java_org_graphlab_Aggregator_initNative
  (JNIEnv *env, jclass clazz){

  // Aggregator#exec
  proxy_aggregator::java_exec =
    env->GetMethodID(clazz, "exec", "(JLorg/graphlab/data/Vertex;)V");
  
  // Aggregator#add
  proxy_aggregator::java_add =
    env->GetMethodID(clazz, "add", "(Lorg/graphlab/Aggregator;)V");
  
  // Aggregator#finalize
  proxy_aggregator::java_finalize = 
    env->GetMethodID(clazz, "finalize", "(J)V");
  
  // Aggregator#clone
  proxy_aggregator::java_clone = 
    env->GetMethodID(clazz, "clone", "()Lorg/graphlab/Aggregator;");
  
}

//---------------------------------------------------------------
// proxy_aggregator instance members
//---------------------------------------------------------------

proxy_aggregator::
  proxy_aggregator(JNIEnv *env, jobject &obj)
  : java_any(env, obj){}

proxy_aggregator::proxy_aggregator()
  : java_any(){}

proxy_aggregator::
  proxy_aggregator(const proxy_aggregator& other) {

  // other doesn't have an existing ref
  if (NULL == other.obj()){
    set_obj(NULL);
    return;
  }
  
  // clone the java object
  JNIEnv *env = core::get_jni_env();
  set_obj(env->CallObjectMethod(other.obj(), java_clone));
  
}

proxy_aggregator &proxy_aggregator::operator=(const proxy_aggregator& other){
    
  if (this == &other) return *this;
  java_any::operator=(other);
  return *this;
  
}

proxy_aggregator::~proxy_aggregator(){}

//---------------------------------------------------------------
// proxy_aggregator instance members - the update function
//---------------------------------------------------------------

void proxy_aggregator::operator()(icontext_type& context){
  
  // forward call to org.graphlab.Aggregator#exec
  JNIEnv *env = core::get_jni_env();
  env->CallVoidMethod (obj(), java_exec,
                       &context,
                       context.vertex_data().obj());
  handle_exception(env);

}

//---------------------------------------------------------------
// proxy_aggregator instance members - the add function
//---------------------------------------------------------------

void proxy_aggregator::operator+=(const proxy_aggregator& other) {
  
  // forward call to org.graphlab.Aggregator#add
  JNIEnv *env = core::get_jni_env();
  env->CallVoidMethod (obj(), java_add, other.obj());
  handle_exception(env);

}

//---------------------------------------------------------------
// proxy_aggregator instance members - the finalize function
//---------------------------------------------------------------

void proxy_aggregator::finalize(iglobal_context& context){

  // forward call to org.graphlab.Aggregator#finalize
  JNIEnv *env = core::get_jni_env();
  env->CallVoidMethod (obj(), java_finalize, &context);
  handle_exception(env);

}
