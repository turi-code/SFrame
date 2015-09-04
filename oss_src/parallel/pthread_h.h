/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_PARALLEL_PTHREAD_INDIRECT_INCLUDE_HPP
#define GRAPHLAB_PARALLEL_PTHREAD_INDIRECT_INCLUDE_HPP
#ifdef _WIN32
#include <parallel/winpthreadsll.h>
#else
#include <pthread.h>
#endif 
#endif
