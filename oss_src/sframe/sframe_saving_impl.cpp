/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <sstream>
#include <sframe/sframe.hpp>
#include <sframe/sarray_v2_block_manager.hpp>
#include <sframe/sarray_v2_block_types.hpp>
namespace graphlab {
namespace sframe_saving_impl {
void advance_column_blocks_to_next_block(
    v2_block_impl::block_manager& block_manager,
    column_blocks& block) {

  ++block.current_block_number;
  if (block.current_block_number >= block.num_blocks_in_current_segment) {
    // we need to advance to the next segment
    // close the current segment address
    block_manager.close_column(block.segment_address);
    while(1) {
      // advance to the next segment if there is one
      block.current_block_number = 0;
      ++block.current_segment_number;
      if (block.current_segment_number < block.num_segments) {
        // open then ext segment
        auto current_segment_file = 
            block.column_index.segment_files[block.current_segment_number];
        block.segment_address = block_manager.open_column(current_segment_file);
        block.num_blocks_in_current_segment = 
            block_manager.num_blocks_in_column(block.segment_address);
        // segment is empty. keep going...
        if (block.num_blocks_in_current_segment == 0) {
          block_manager.close_column(block.segment_address);
          continue;
        }
        break;
      } else {
        block.eof = true;
        break;
      }
    }
  }
}
} // sframe_saving_impl
} // namespace graphlab
