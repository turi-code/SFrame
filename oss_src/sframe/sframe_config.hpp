/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_SFRAME_CONFIG_HPP
#define GRAPHLAB_SFRAME_CONFIG_HPP
#include <cstddef>
namespace graphlab {

/**
** Global configuration for sframe, keep them as non-constants because we want to
** allow user/server to change the configuration according to the environment
**/
namespace sframe_config {
  /**
  **  The max buffer size to keep for sorting in memory
  **/
  extern size_t SFRAME_SORT_BUFFER_SIZE;

  /**
  **  The number of rows to read each time for paralleliterator
  **/
  extern size_t SFRAME_READ_BATCH_SIZE;
}

}
#endif //GRAPHLAB_SFRAME_CONFIG_HPP
