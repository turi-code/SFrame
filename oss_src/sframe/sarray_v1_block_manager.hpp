/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_SFRAME_SARRAY_V1_BLOCK_MANAGER_HPP
#define GRAPHLAB_SFRAME_SARRAY_V1_BLOCK_MANAGER_HPP
#include <stdint.h>
#include <vector>
#include <fstream>
#include <parallel/pthread_tools.hpp>
#include <fileio/general_fstream.hpp>
#include <sframe/sarray_index_file.hpp>
namespace graphlab {
namespace v1_block_impl {
/**
 * The header of the block
 */
struct block_header {
  // The number of elements in this block
  uint64_t num_elements = 0;
  // The number of bytes in the block. Excludes the header.
  uint64_t num_bytes = 0;
  // block flags, describing the properties of the block 
  uint64_t flags = 0;
};

enum BLOCK_FLAGS {
  LZ4_COMPRESSION = 1
};

/**
 * Provides information about blocks inside an SArray.
 * Provides ability to read entire blocks.
 * A block address is a pair of segment ID and a sequential block number within
 * the segment. For instance, the third block in segment 0 is 
 * block_reader::block_id{0, 2}.
 * This class manages block reading of an SArray, and provides functions to
 * query the blocks (such has how many blocks are there in the segment, and
 * how many rows are there in the block, where to find a particular row, etc).
 */
class block_reader {
 public:
  /// A block address is a pair of segment ID and block number within the segment
  typedef std::pair<size_t, size_t> block_id;

  /// default constructor. Does nothing.
  inline block_reader() {};

  /// Constructs the block information using the array index information
  void init(index_file_information index);

  /** Returns the segment ID containing the row. 
   * Returns (size_t)(-1) on failure.
   */
  size_t segment_containing_row(size_t row);

  /** Returns a pair of (segment ID, block offset) containing the row. 
   * Returns (size_t)(-1) on failure.
   */
  block_id block_containing_row(size_t row);

  /** Returns the first row of a block
   * Returns (size_t)(-1) on failure.
   */
  size_t first_row_of_block(block_id segment_and_block); 

  /** Returns the number of blocks in a segment
   */
  size_t num_blocks_in_segment(size_t segmentid); 

  /** Returns the number of rows in a block
   * Returns (size_t)(-1) on failure.
   */
  size_t num_elem_in_block(block_id segment_and_block); 

  /** Reads a block given a (segment ID, block offset) pair,
   *  into an in memory buffer (c of size len). The buffer must be 
   *  sized to be at least "block_size" bytes. Returns the number of bytes read.
   *  Returns (size_t)(-1) on failure.
   *
   *  Safe for concurrent operation.
   */
  size_t read_block(block_id segment_and_block, char* c);

  /** Reads a collection of blocks given as (segment ID, block offset) pairs,
   *  into an memory buffers (c of size len). Each buffer must be 
   *  sized to be at least "block_size" bytes.  The returned vector is the same
   *  length as segment_and_block, and contains the number of bytes written
   *  into each block. Contains (size_t)(-1) on failure.
   *
   *  Equivalent to issuing multiple calls to read_block(), one for each 
   *  requested block, but possibly faster.
   *
   *  Safe for concurrent operation.
   */
  std::vector<size_t> read_blocks(std::vector<block_id> segment_and_block, 
                                  std::vector<char*> c);
 private:
  struct block_info {
    size_t offset = 0; /// The file offsets of the block
    size_t length = 0; /// The length of the block in bytes on disk
    size_t start_row = 0; /// The start row of the block
    size_t num_elem = 0; /// The number of elements in the block
    size_t flags = 0;  /// block flags
  };
  struct segment_info {
    mutex lock; // acquires a lock for this struct for filling out the
                // block_offsets and block_row_ids. Once it is filled
                // this struct is never modified and the lock never needs to be
                // acquired again
    bool segment_loaded = false; // true if row_id_to_block is loaded
    size_t start_row = 0; // The first row number in this segment
    size_t last_row = 0;  // one past the last row number in this segment
    size_t num_rows = 0;  // The number of rows in this segment 
                          // (last_row - start_row)
    std::vector<block_info> blocks;
  };
  index_file_information index_info;
  std::vector<segment_info> segments;

  /** fills the block_offsets and block_row_ids of the segments[segmentid]
   *  struct if not already filled. If already filled, this does nothing.
   */
  void load_segment_block_info(size_t segmentid);


  /**
   * Holds the file handle to the segment. 
   * Will also contain caches and stuff later.
   */
  struct segment_io_data {
    mutex lock;
    // really... I wanted a unique_ptr here. But Mac's C++11 implementation
    // is incomplete and vectors of unique_ptr are somewhat broken.
    general_ifstream* fin = NULL;
    std::vector<char> compression_buffer;
    ~segment_io_data() {
      if (fin) delete fin;
    }
  };
  std::vector<segment_io_data> segment_io;
};





class block_writer {
 public:

  void set_num_segments(size_t num_segments);

  void open_segment(size_t segmentid,
                    std::string filename);

  /**
   * Writes a block of data into a segment.
   *
   * \param segmentid The segment to write to
   * \param data Pointer to the data to write
   * \param len The length of the data
   * \param num_elements The number of rows in the data
   * \param flags Additional flags of the block;
   */
  void write_block(size_t segmentid,
                   char* data,
                   size_t len,
                   size_t num_elements,
                   uint64_t flags);

  void close_segment(size_t segmentid);
 private:
  // The output files for each open segment
  std::vector<std::vector<char> > compression_buffer;
  // The output files for each open segment
  std::vector<std::unique_ptr<general_ofstream> > output_files;
  // a vector of all the block information is stuck in the footer of the file
  std::vector<std::vector<v1_block_impl::block_header> > all_block_information;

  /// Writes the file footer
  void emit_footer(size_t segmentid);
};




} // namespace v1_block_impl
} // namespace graphlab
#endif
