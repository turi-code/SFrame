/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_SFRAME_SARRAY_V2_BLOCK_TYPES_HPP
#define GRAPHLAB_SFRAME_SARRAY_V2_BLOCK_TYPES_HPP
#include <stdint.h>
#include <tuple>
#include <serialization/serializable_pod.hpp>
namespace graphlab {
namespace v2_block_impl {


enum BLOCK_FLAGS {
  LZ4_COMPRESSION = 1,
  IS_FLEXIBLE_TYPE = 2,
  MULTIPLE_TYPE_BLOCK = 4,
  BLOCK_ENCODING_EXTENSION = 8  // used to flag secondary compression schemes
};

namespace DOUBLE_RESERVED_FLAGS {
enum FLAGS {
  LEGACY_ENCODING = 0,
  INTEGER_ENCODING = 1
};
}


namespace VECTOR_RESERVED_FLAGS {
enum FLAGS {
  NEW_ENCODING = 0
};
}
/**
 * A column address is a tuple of segment_id, 
 * column number within the segment
 */
typedef std::tuple<size_t, size_t> column_address;

/**
 * A block address is a tuple of segment_id, 
 * column number, block number within the segment
 */
typedef std::tuple<size_t, size_t, size_t> block_address;

/** 
 * Metadata about each block
 */
struct block_info: public graphlab::IS_POD_TYPE {
  uint64_t offset = (uint64_t)(-1); /// The file offsets of the block
  uint64_t length = 0; /// The length of the block in bytes on disk
  /** 
   * The decompressed length of the block in bytes 
   * on disk. Only different from length if the block is LZ4_compressed. 
   */
  uint64_t block_size = 0; 
  uint64_t num_elem = 0; /// The number of elements in the block
  uint64_t flags = 0;  /// block flags
  /**
   * If flags & IS_FLEXIBLE_TYPE, the type of the contents.
   * This is really of type flex_type_enum
   */
  uint16_t content_type = 0;
};

} // v2_block_impl
} // graphlab

#endif
