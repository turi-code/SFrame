/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <parallel/pthread_tools.hpp>

namespace graphlab {

/** This is needed to patch some issues with the local inproc cluster
 *  use; the constructor of a distributed object do not appear to be
 *  threadsafe; this seems to make it work reliably.
 */
mutex distributed_object_construction_lock;

}
