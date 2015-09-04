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
 * @file java_any.cpp
 * @author Jiunn Haur Lim <jiunnhal@cmu.edu>
 */ 
 
#include <graphlab.hpp>
#include "java_any.hpp"
#include "org_graphlab_Core.hpp"
#include "org_graphlab_Updater.hpp"

using namespace graphlab;

//---------------------------------------------------------------
// java_any instance members
//---------------------------------------------------------------

java_any::java_any(JNIEnv *env, jobject &obj){
  // create a new ref so that it doesn't get garbage collected
  mobj = env->NewGlobalRef(obj);
}

java_any::java_any() : mobj(NULL){}

jobject &java_any::obj() {
  return mobj;
}

const jobject &java_any::obj() const {
  return mobj;
}

java_any::java_any(const java_any& other){
    
  // other doesn't have an existing ref
  if (NULL == other.mobj){
    this->mobj = NULL;
    return;
  }
  
  // create a new ref
  JNIEnv *env = proxy_updater::core::get_jni_env();
  this->mobj = env->NewGlobalRef(other.mobj);
  
}

java_any &java_any::operator=(const java_any& other){
    
  // prevent self assignment
  if (this == &other) return *this;
  
  JNIEnv *env = proxy_updater::core::get_jni_env();
  jobject obj = NULL;
  
  // if other has a java object, create a new ref
  if (NULL != other.mobj){
    obj = env->NewGlobalRef(other.mobj);
  }
  
  // if this has a java object, delete ref
  if (NULL != this->mobj){
    env->DeleteGlobalRef(this->mobj);
  }
    
  // assign!
  this->mobj = obj;
  
  return *this;
  
}

void java_any::set_obj(jobject obj){

  JNIEnv *env = proxy_updater::core::get_jni_env();
  
  if (NULL != mobj){
    // delete current ref
    env->DeleteGlobalRef(mobj);
    mobj = NULL;
  }
  
  if (NULL != obj){
    mobj = env->NewGlobalRef(obj);
  }
  
}

java_any::~java_any(){
  if (NULL == mobj) return;
  // delete reference to allow garbage collection
  JNIEnv *env = proxy_updater::core::get_jni_env();
  env->DeleteGlobalRef(mobj);
  mobj = NULL;
}

bool java_any::handle_exception(JNIEnv *env) const {
  
  // check for exception
  jthrowable exc = env->ExceptionOccurred();
  if (!exc) return false;

  env->ExceptionDescribe();
  env->ExceptionClear();
  proxy_updater::core::throw_exception(
        env,
        "java/lang/IllegalArgumentException",
        "thrown from C code.");
  
  return true;
  
}
