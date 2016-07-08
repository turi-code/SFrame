/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#ifndef GRAPHLAB_UNITY_VERSION_HPP
#define GRAPHLAB_UNITY_VERSION_HPP
#include "version_number.hpp"

#ifdef __UNITY_VERSION__
#define UNITY_VERSION __UNITY_VERSION__
#else
#define UNITY_VERSION "0.1.internal"
#endif
#endif
