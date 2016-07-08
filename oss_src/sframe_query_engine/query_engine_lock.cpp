/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <parallel/mutex.hpp>
namespace graphlab {
namespace query_eval {
recursive_mutex global_query_lock;
} // query_eval
} // graphlab
