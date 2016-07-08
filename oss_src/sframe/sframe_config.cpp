/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <cmath>
#include <cstddef>
#include <globals/globals.hpp>
#include "export.hpp"

namespace graphlab {

/**
** Global configuration for sframe, keep them as non-constants because we want to
** allow user/server to change the configuration according to the environment
**/
namespace sframe_config {
EXPORT size_t SFRAME_SORT_BUFFER_SIZE = size_t(2*1024*1024)*size_t(1024);
EXPORT size_t SFRAME_READ_BATCH_SIZE = 128;

REGISTER_GLOBAL_WITH_CHECKS(int64_t, 
                            SFRAME_SORT_BUFFER_SIZE,
                            true, 
                            +[](int64_t val){ return (val >= 1024) &&
                            // Check against overflow...no more than an exabyte
                            (val <= size_t(1024*1024*1024)*size_t(1024*1024*1024)); });


REGISTER_GLOBAL_WITH_CHECKS(int64_t, 
                            SFRAME_READ_BATCH_SIZE, 
                            true, 
                            +[](int64_t val){ return val >= 1; });

}
}
