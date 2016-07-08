/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_SFRAME_SAVING_IMPL_HPP
#define GRAPHLAB_SFRAME_SAVING_IMPL_HPP
#include <sframe/sarray_v2_block_types.hpp>
namespace graphlab {
namespace sframe_saving_impl {
/**
 *  Represents the writing state of a single column; which
 *  segment, and which block within the segment it is at.
 */
struct column_blocks {
  // index for this column
  index_file_information column_index;
  // column number this gets written into
  size_t column_number = 0;

  // total number of segments in this column
  size_t num_segments = 0;
  // total number of columns in the current segment
  size_t num_blocks_in_current_segment = 0;

  size_t current_segment_number = 0;
  size_t current_block_number = 0;

  // reference to the opened segment
  v2_block_impl::column_address segment_address;

  // the next row number to be read
  size_t next_row = 0;

  bool eof = false;
};

/**
 * Advances the column block to the next block.
 */
void advance_column_blocks_to_next_block(
    v2_block_impl::block_manager& block_manager,
    column_blocks& block);
} // sframe_saving_impl
} // graphlab
#endif
