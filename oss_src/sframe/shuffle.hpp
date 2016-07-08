/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_SFRAME_SHUFFLE_HPP
#define GRAPHLAB_SFRAME_SHUFFLE_HPP

#include <vector>
#include <sframe/sframe.hpp>

namespace graphlab {

/**
 * Shuffle the rows in one sframe into a collection of n sframes.
 * Each output SFrame contains one segment.
 *
 * \code
 * std::vector<sframe> ret(n);
 * for (auto& sf : ret) {
 *   INIT_WITH_NAMES_COLUMNS_AND_ONE_SEG(sframe_in.column_names(), sframe_in.column_types());
 * }
 * for (auto& row : sframe_in) {
 *   size_t idx = hash_fn(row) % n;
 *   add_row_to_sframe(ret[idx], row); // the order of addition is not guaranteed.
 * }
 * \endcode
 *
 * The result sframes have the same column names and types (including
 * empty sframes). A result sframe can have 0 rows if non of the
 * rows in the input sframe is hashed to it. (If n is greater than
 * the size of input sframe, there will be at (n - sframe_in.size())
 * empty sframes in the return vector.
 *
 * \param n the number of output sframe.
 * \param hash_fn the hash function for each row in the input sframe.
 * 
 * \return A vector of n sframes.
 */
std::vector<sframe> shuffle(
     sframe sframe_in,
     size_t n,
     std::function<size_t(const std::vector<flexible_type>&)> hash_fn,
     std::function<void(const std::vector<flexible_type>&, size_t)> emit_call_back
      = std::function<void(const std::vector<flexible_type>&, size_t)>());
}
#endif
