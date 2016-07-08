/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <lambda/lambda_constants.hpp>
#include <graphlab/util/bitops.hpp>
#include <globals/globals.hpp>

namespace graphlab {

size_t DEFAULT_NUM_PYLAMBDA_WORKERS = 16;

size_t DEFAULT_NUM_GRAPH_LAMBDA_WORKERS = 16;

REGISTER_GLOBAL_WITH_CHECKS(int64_t,
                            DEFAULT_NUM_PYLAMBDA_WORKERS,
                            true, 
                            +[](int64_t val){ return val >= 1; });

REGISTER_GLOBAL_WITH_CHECKS(int64_t,
                            DEFAULT_NUM_GRAPH_LAMBDA_WORKERS,
                            true, 
                            +[](int64_t val){ return val >= 1; });
}
