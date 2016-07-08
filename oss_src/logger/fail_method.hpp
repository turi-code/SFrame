/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

/*  
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


#ifndef GRAPHLAB_LOGGER_FAIL_METHOD
#include <string>

#ifdef GRAPHLAB_LOGGER_THROW_ON_FAILURE
#define GRAPHLAB_LOGGER_FAIL_METHOD(str) throw(str)
#define LOGGED_GRAPHLAB_LOGGER_FAIL_METHOD(str) log_and_throw(str)
#else
#define GRAPHLAB_LOGGER_FAIL_METHOD(str) abort()
#define LOGGED_GRAPHLAB_LOGGER_FAIL_METHOD(str) abort()
#endif

#endif
