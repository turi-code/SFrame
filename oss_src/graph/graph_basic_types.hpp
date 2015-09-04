/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
/**
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


#ifndef GRAPHLAB_GRAPH_BASIC_TYPES
#define GRAPHLAB_GRAPH_BASIC_TYPES

#include <stdint.h>

namespace graphlab {




#ifdef USE_VID32
  /// Identifier type of a vertex which is globally consistent. Guaranteed to be integral
  typedef uint32_t vertex_id_type;
#else
  typedef uint64_t vertex_id_type;
#endif

  /// Identifier type of a vertex which is only locally consistent. Guaranteed to be integral
  typedef vertex_id_type lvid_type;

  /**
   * Identifier type of an edge which is only locally
   * consistent. Guaranteed to be integral and consecutive.
   */
  typedef lvid_type edge_id_type;

  /**
   * \brief The set of edges that are traversed during gather and scatter
   * operations.
   */
  enum edge_dir_type {
    /**
     * \brief No edges implies that no edges are processed during the
     * corresponding gather or scatter phase, essentially skipping
     * that phase.
     */
    NO_EDGES = 0,
    /**
     * \brief In edges implies that only whose target is the center
     * vertex are processed during gather or scatter.
     */
    IN_EDGES = 1,
    /**
     * \brief Out edges implies that only whose source is the center
     * vertex are processed during gather or scatter.
     */
    OUT_EDGES = 2 ,
    /**
     * \brief All edges implies that all adges adjacent to a the
     * center vertex are processed on gather or scatter.  Note that
     * some neighbors may be encountered twice if there is both an in
     * and out edge to that neighbor.
     */
    ALL_EDGES = 3};
} // end of namespace graphlab

#endif
