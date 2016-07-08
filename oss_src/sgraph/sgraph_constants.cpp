/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <sgraph/sgraph_constants.hpp>
#include <graphlab/util/bitops.hpp>
#include <globals/globals.hpp>
#include <parallel/pthread_tools.hpp>
#include "export.hpp"

namespace graphlab {

EXPORT size_t SGRAPH_TRIPLE_APPLY_LOCK_ARRAY_SIZE = 1024 * 1024;
EXPORT size_t SGRAPH_BATCH_TRIPLE_APPLY_LOCK_ARRAY_SIZE = 1024 * 1024;
EXPORT size_t SGRAPH_TRIPLE_APPLY_EDGE_BATCH_SIZE = 1024;
EXPORT size_t SGRAPH_DEFAULT_NUM_PARTITIONS = 8;
EXPORT size_t SGRAPH_INGRESS_VID_BUFFER_SIZE = 1024 * 1024 * 1;
EXPORT size_t SGRAPH_HILBERT_CURVE_PARALLEL_FOR_NUM_THREADS = thread::cpu_count();

REGISTER_GLOBAL_WITH_CHECKS(int64_t, 
                            SGRAPH_TRIPLE_APPLY_LOCK_ARRAY_SIZE, 
                            true, 
                            +[](int64_t val){ return val >= 1; });


REGISTER_GLOBAL_WITH_CHECKS(int64_t, 
                            SGRAPH_BATCH_TRIPLE_APPLY_LOCK_ARRAY_SIZE, 
                            true, 
                            +[](int64_t val){ return val >= 1; });

REGISTER_GLOBAL_WITH_CHECKS(int64_t, 
                            SGRAPH_TRIPLE_APPLY_EDGE_BATCH_SIZE, 
                            true, 
                            +[](int64_t val){ return val >= 1; });


REGISTER_GLOBAL_WITH_CHECKS(int64_t, 
                            SGRAPH_DEFAULT_NUM_PARTITIONS, 
                            true, 
                            +[](int64_t val){ return val >= 1 && is_power_of_2((uint64_t)val); });

REGISTER_GLOBAL_WITH_CHECKS(int64_t, 
                            SGRAPH_INGRESS_VID_BUFFER_SIZE, 
                            true, 
                            +[](int64_t val){ return val >= 1; });

REGISTER_GLOBAL_WITH_CHECKS(int64_t, 
                            SGRAPH_HILBERT_CURVE_PARALLEL_FOR_NUM_THREADS,
                            true,
                            +[](int64_t val){ return val >= 1; });
}
