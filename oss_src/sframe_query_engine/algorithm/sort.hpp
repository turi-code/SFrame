/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_QUERY_EVAL_SORT_HPP
#define GRAPHLAB_QUERY_EVAL_SORT_HPP

#include <vector>
#include <memory>

namespace graphlab {

class sframe;

namespace query_eval {

class planner_node;

/**
 * Sort given SFrame.
 *
 * The algorithm is like the following:
 *   - First do a quantile sketch over all sort columns and use the quantile sketch to
 *     figure out the partition keys that we will use to split the sframe rows into
 *     small chunks so that each chunk is realtively sorted. Each chunk is small enough
 *     so that we could sort in memory 
 *   - Scatter partition the sframe according to above partition keys. The resulting
 *     value is persisted. Each partition is stored as one segment in a sarray.
 *   - The sorting resulting is then lazily materialized through le_sort operator
 *
 * There are a few optimizations along the way:
 *   - if all sorting keys are the same, then no need to sort
 *   - if the sframe is small enough to fit into memory, then we simply do a in
 *     memory sort
 *   - if some partitions of the sframe have the same sorting key, then that partition
 *     will not be sorted
 *
 * \param sframe_planner_node The lazy sframe to be sorted
 * \param sort_column_names The columns to be sorted
 * \param sort_orders The order for each column to be sorted, true is ascending
 * \return The sorted sframe
 **/
std::shared_ptr<sframe> sort(
    std::shared_ptr<planner_node> sframe_planner_node,
    const std::vector<std::string> column_names,
    const std::vector<size_t>& sort_column_indices,
    const std::vector<bool>& sort_orders);

} // end of query_eval
} // end of graphlab


#endif //GRAPHLAB_SFRAME_SORT_HPP
