/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef SFRAME_ALGORITHM_EC_PERMUTE_HPP
#define SFRAME_ALGORITHM_EC_PERMUTE_HPP


#include <vector>
#include <memory>
/**
 * See ec_sort.hpp for detauls
 */
namespace graphlab {
class sframe;

template <typename T>
class sarray;

namespace query_eval {

/**
 * Permutes an sframe by a forward map.
 * forward_map has the same length as the sframe and must be a permutation
 * of all the integers [0, len-1].
 *
 * The input sframe is then permuted so that sframe row i is written to row
 * forward_map[i] of the returned sframe.
 *
 * \note The forward_map is not checked that it is a valid permutation
 * If the constraints is not met, either an exception will be thrown, or
 * the result is ill-defined.
 */
sframe permute_sframe(sframe &values_sframe,
                      std::shared_ptr<sarray<flexible_type> > forward_map);
} // query_eval
} // graphlab

#endif
