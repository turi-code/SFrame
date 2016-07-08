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

#include "org_graphlab_Updater.hpp"

using namespace graphlab;

//---------------------------------------------------------------
// proxy_updater static members
//---------------------------------------------------------------

jmethodID proxy_updater::java_update                  = 0;
jmethodID proxy_updater::java_add                     = 0;
jmethodID proxy_updater::java_priority                = 0;
jmethodID proxy_updater::java_clone                   = 0;
jmethodID proxy_updater::java_is_factorizable         = 0;
jmethodID proxy_updater::java_gather_edges            = 0;
jmethodID proxy_updater::java_scatter_edges           = 0;
jmethodID proxy_updater::java_consistency             = 0;
jmethodID proxy_updater::java_gather_consistency      = 0;
jmethodID proxy_updater::java_scatter_consistency     = 0;
jmethodID proxy_updater::java_init_gather             = 0;
jmethodID proxy_updater::java_gather                  = 0;
jmethodID proxy_updater::java_merge                   = 0;
jmethodID proxy_updater::java_apply                   = 0;
jmethodID proxy_updater::java_scatter                 = 0;

JNIEXPORT void JNICALL
  Java_org_graphlab_Updater_initNative
  (JNIEnv *env, jclass clazz){

  proxy_updater::java_update =
    env->GetMethodID(clazz, "update", "(JLorg/graphlab/data/Vertex;)V");

  proxy_updater::java_add =
    env->GetMethodID(clazz, "add", "(Lorg/graphlab/Updater;)V");

  proxy_updater::java_priority = 
    env->GetMethodID(clazz, "priority", "()D");

  proxy_updater::java_clone = 
    env->GetMethodID(clazz, "clone", "()Lorg/graphlab/Updater;");

  proxy_updater::java_is_factorizable =
    env->GetMethodID(clazz, "isFactorizable", "()Z");

  proxy_updater::java_gather_edges =
    env->GetMethodID(clazz, "gatherEdges", "()I");

  proxy_updater::java_scatter_edges =
    env->GetMethodID(clazz, "scatterEdges", "()I");

  proxy_updater::java_consistency =
    env->GetMethodID(clazz, "consistency", "()I");

  proxy_updater::java_gather_consistency =
    env->GetMethodID(clazz, "gatherConsistency", "()I");

  proxy_updater::java_scatter_consistency =
    env->GetMethodID(clazz, "scatterConsistency", "()I");

  proxy_updater::java_init_gather = 
    env->GetMethodID(clazz, "initGather", "()V");

  proxy_updater::java_gather =
    env->GetMethodID(clazz, "gather", "(Ljava/lang/Object;)V");

  proxy_updater::java_merge = 
    env->GetMethodID(clazz, "merge", "(Lorg/graphlab/Updater;)V");

  proxy_updater::java_apply =
    env->GetMethodID(clazz, "apply", "(Lorg/graphlab/data/Vertex;)V");

  proxy_updater::java_scatter = 
    env->GetMethodID(clazz, "scatter",
    "(JLjava/lang/Object;)V");
  
}

//---------------------------------------------------------------
// proxy_updater instance members
//---------------------------------------------------------------

proxy_updater::
  proxy_updater(JNIEnv *env, jobject &obj)
  : java_any(env, obj){}

proxy_updater::proxy_updater(){}

proxy_updater::
  proxy_updater(const proxy_updater& other){

  // other doesn't have an existing ref
  if (NULL == other.obj()){
    set_obj(NULL);
    return;
  }
  
  // clone the java object
  JNIEnv *env = core::get_jni_env();
  set_obj(env->CallObjectMethod(other.obj(), java_clone));
  
}

proxy_updater &proxy_updater::operator=(const proxy_updater& other){
    
  if (this == &other) return *this;
  java_any::operator=(other);
  return *this;
  
}

proxy_updater::~proxy_updater(){}

//---------------------------------------------------------------
// proxy_updater instance members - the update function
//---------------------------------------------------------------

void proxy_updater::operator()(icontext_type& context){
  
  jobject vertex = context.const_vertex_data().obj();
  if (NULL == vertex) return; // BUG?
  
  // forward call to org.graphlab.Updater#update
  JNIEnv *env = core::get_jni_env();
  env->CallVoidMethod (obj(), java_update,
                       &context,
                       vertex);
  handle_exception(env);

}

//---------------------------------------------------------------
// proxy_updater instance members - the add function
//---------------------------------------------------------------

void proxy_updater::operator+=(const proxy_updater& other) const {

  // forward call to org.graphlab.Updater#add
  JNIEnv *env = core::get_jni_env();
  env->CallVoidMethod (obj(), java_add, other.obj());
  handle_exception(env);

}

bool proxy_updater::is_factorizable() const {
  JNIEnv *env = core::get_jni_env();
  bool factorizable = env->CallBooleanMethod(obj(), java_is_factorizable);
  handle_exception(env);
  return factorizable;
}

edge_set proxy_updater::gather_edges() const {
  JNIEnv *env = core::get_jni_env();
  int e = env->CallIntMethod(obj(), java_gather_edges);
  handle_exception(env);
  switch(e){
    case 0:  return IN_EDGES;
    case 1:  return OUT_EDGES;
    case 2:  return ALL_EDGES;
    default: return NO_EDGES;
  }
}

edge_set proxy_updater::scatter_edges() const {
  JNIEnv *env = core::get_jni_env();
  int e = env->CallIntMethod(obj(), java_scatter_edges);
  handle_exception(env);
  switch(e){
    case 0:  return IN_EDGES;
    case 1:  return OUT_EDGES;
    case 2:  return ALL_EDGES;
    default: return NO_EDGES;
  }
}

consistency_model proxy_updater::consistency() const {
  JNIEnv *env = core::get_jni_env();
  int c = env->CallIntMethod(obj(), java_consistency);
  handle_exception(env);
  switch(c){
    case 0:  return NULL_CONSISTENCY;
    case 1:  return VERTEX_CONSISTENCY;
    case 2:  return EDGE_CONSISTENCY;
    case 3:  return FULL_CONSISTENCY;
    default: return DEFAULT_CONSISTENCY;
  }
}

consistency_model proxy_updater::gather_consistency() const {
  JNIEnv *env = core::get_jni_env();
  int c = env->CallIntMethod(obj(), java_gather_consistency);
  handle_exception(env);
  switch(c){
    case 0:  return NULL_CONSISTENCY;
    case 1:  return VERTEX_CONSISTENCY;
    case 2:  return EDGE_CONSISTENCY;
    case 3:  return FULL_CONSISTENCY;
    default: return DEFAULT_CONSISTENCY;
  }
}

consistency_model proxy_updater::scatter_consistency() const {
  JNIEnv *env = core::get_jni_env();
  int c = env->CallIntMethod(obj(), java_scatter_consistency);
  handle_exception(env);
  switch(c){
    case 0:  return NULL_CONSISTENCY;
    case 1:  return VERTEX_CONSISTENCY;
    case 2:  return EDGE_CONSISTENCY;
    case 3:  return FULL_CONSISTENCY;
    default: return DEFAULT_CONSISTENCY;
  }
}

void proxy_updater::init_gather(icontext_type& context) {
  JNIEnv *env = core::get_jni_env();
  env->CallVoidMethod(obj(), java_init_gather);
  handle_exception(env);
}

void proxy_updater::gather(icontext_type& context, const edge_type& edge){
  JNIEnv *env = core::get_jni_env();
  env->CallVoidMethod(obj(), java_gather, context.const_edge_data(edge).obj());
  handle_exception(env);
}
 
void proxy_updater::merge(const update_functor_type& other){
  JNIEnv *env = core::get_jni_env();
  env->CallVoidMethod(obj(), java_merge, other.obj());
  handle_exception(env);
}

void proxy_updater::apply(icontext_type& context){
  jobject vertex = context.const_vertex_data().obj();
  if (NULL == vertex) return; // BUG?
  JNIEnv *env = core::get_jni_env();
  env->CallVoidMethod(obj(), java_apply, vertex);
  handle_exception(env);
}
 
void proxy_updater::scatter(icontext_type& context, const edge_type& edge){
  JNIEnv *env = core::get_jni_env();
  env->CallVoidMethod(obj(), java_scatter,
    &context, context.const_edge_data(edge).obj());
  handle_exception(env);
}
