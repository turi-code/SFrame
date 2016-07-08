/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <string>
#include <vector>
#include <cstdio>
#include <boost/algorithm/string.hpp>
#include <sframe/sframe_constants.hpp>
#include <sframe/sframe.hpp>
#include <sframe/join_impl.hpp>

namespace graphlab {

sframe join(sframe& sf_left,
            sframe& sf_right,
            std::string join_type,
            const std::map<std::string,std::string> join_columns,
            size_t max_buffer_size = SFRAME_JOIN_BUFFER_NUM_CELLS);

} // end of graphlab
