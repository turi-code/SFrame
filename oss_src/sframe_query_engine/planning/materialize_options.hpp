/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_SFRAME_QUERY_ENGINE_MATERIALIZE_OPTIONS_HPP
#define GRAPHLAB_SFRAME_QUERY_ENGINE_MATERIALIZE_OPTIONS_HPP
#include <memory>
#include <cstddef>
#include <functional>
#include <vector>
#include <string>

namespace graphlab {
class sframe_rows;
namespace query_eval {

/**  
 * Materialization options.
 *
 * This options can be used to control each stage of the materialization 
 * pipeline.  
 */
struct materialize_options {

 /** 
  *  The number of segments to break parallel processing into. Also
  *  may affect the number of segments of the output SFrame.
  */
  size_t num_segments = 0;

  /**     
   * If set, the final sframe output will be streamed into the callback
   * function and an empty SFrame will be returned.
   *
   * The type of the callback function is
   * \code
   *   std::function<bool(size_t, const std::shared_ptr<sframe_rows>&)>,
   * \endcode
   *
   * where the first argument is the segment_id being processed, and
   * the rest is the data. If true is returned, then the processing
   * is stopped.
   */
  std::function<bool(size_t, const std::shared_ptr<sframe_rows>&)> write_callback;

  /**
   * Disables query optimizations.
   */
  bool disable_optimization = false;

  /**
   * If Optimizations are enabled, enabling this will only
   * run the first pass optimizations: project/union reordering.
   */
  bool only_first_pass_optimizations = false;

 /**
  * If true, then the naive materialize algorithm will be run.
  * All nodes will be explicitly materialized, and no
  * optimization will be performed.  Useful for error checking
  * the optimizations.
  */
  bool naive_mode = false;

  /**
   * If true, the materialization algorithm will partially materialize
   * the query plan until all remaining paths are linearly consumable.
   *
   * For successful query execution, this should always be true. When this is
   * false, query execution may fail for particular types of plans due to 
   * rate control issues.
   */
  bool partial_materialize = true;

  /**     
   * if set, these parameter defines the sframe output index file location
   * of the final sframe. Also see \ref output_column_names  
   * This argument has no effect if \ref write_callback is set.
   */
  std::string output_index_file = "";

  /**     
   * if set, this parameter defines the column names of the output sframe.
   * Otherwise X1,X2,X3... is used.
   * of the final sframe. Also see \ref output_index_file. 
   * This argument has no effect if \ref write_callback is set.
   */
  std::vector<std::string> output_column_names;
};

} // query_eval
} // graphlab
#endif
