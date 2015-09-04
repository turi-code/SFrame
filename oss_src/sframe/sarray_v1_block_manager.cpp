/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
extern "C" {
#include <lz4/lz4.h>
}
#include <algorithm>
#include <parallel/mutex.hpp>
#include <sframe/sarray_v1_block_manager.hpp>

namespace graphlab {
namespace v1_block_impl {
mutex iolock;
/**************************************************************************/
/*                                                                        */
/*                           block_reader class                           */
/*                                                                        */
/**************************************************************************/

void block_reader::init(index_file_information index) {
  index_info = index;
  segments.clear();
  segment_io.clear();
  segments.resize(index.nsegments);
  segment_io.resize(index.nsegments);
  size_t start_row = 0;
  // fill the start row and the num_rows
  for (size_t i = 0;i < segments.size(); ++i) {
    segments[i].start_row = start_row;
    segments[i].num_rows = index.segment_sizes[i];
    segments[i].last_row = segments[i].start_row + segments[i].num_rows;
    start_row += index.segment_sizes[i];
  }
}


size_t block_reader::segment_containing_row(size_t row) {
  for (size_t i = 0;i < segments.size(); ++i) {
    if (segments[i].start_row <= row && row < segments[i].last_row) {
      return i;
    }
  }
  return (size_t)(-1);
}


block_reader::block_id block_reader::block_containing_row(size_t row) {
  size_t segmentid = segment_containing_row(row);
  if (segmentid == (size_t)(-1)) return block_id{(size_t)(-1),(size_t)(-1)};
  load_segment_block_info(segmentid);

  // take a reference to the block information in this segment 
  // for convenience
  auto& blocks = segments[segmentid].blocks;

  // search in block start row
  auto pos = std::lower_bound(blocks.begin(), blocks.end(), row,
                              [](const block_info& a, size_t b) {
                                return a.start_row < b;
                              });
  size_t blocknum = std::distance(blocks.begin(), pos);
  // common case
  if (blocknum < blocks.size()) {
    // the block containing this could be either at blocknum, or blocknum - 1
    if (blocks[blocknum].start_row == row) {
      return block_id(segmentid, blocknum);
    } else {
      return block_id(segmentid, blocknum - 1);
    }
  } else {
    // the last block
    return block_id(segmentid, blocks.size() - 1);
  }
}

size_t block_reader::first_row_of_block(block_id segment_and_block) {
  size_t segmentid = segment_and_block.first;
  size_t blockid = segment_and_block.second;
  load_segment_block_info(segmentid);
  ASSERT_LT(blockid, segments[segmentid].blocks.size());

  return segments[segmentid].blocks[blockid].start_row;
}


size_t block_reader::num_blocks_in_segment(size_t segmentid) {
  load_segment_block_info(segmentid);
  return segments[segmentid].blocks.size();
}

size_t block_reader::num_elem_in_block(block_id segment_and_block) {
  size_t segmentid = segment_and_block.first;
  size_t blockid = segment_and_block.second;
  load_segment_block_info(segmentid);

  ASSERT_LT(blockid, segments[segmentid].blocks.size());

  return segments[segmentid].blocks[blockid].num_elem;
}

size_t block_reader::read_block(block_id segment_and_block, char* c) {
  return read_blocks(std::vector<block_id>{segment_and_block}, 
                     std::vector<char*>{c})[0];
}



std::vector<size_t> 
block_reader::read_blocks(std::vector<block_id> segment_and_block, 
                          std::vector<char*> c) {
  std::vector<size_t> ret(segment_and_block.size(), (size_t)(-1));
  
  size_t last_segment_locked = (size_t)(-1);

  /*
   * Read a sequence of blocks into memory one at a time.
   * However, avoid releasing the segment locks whenever possible. 
   * keeping them for as long as I am reading from the segment
   */
  for (size_t i = 0;i < segment_and_block.size(); ++i) {
    size_t segmentid = segment_and_block[i].first;
    size_t blockid = segment_and_block[i].second;
    load_segment_block_info(segmentid);

    // acquire lock for the io data
    if (last_segment_locked != segmentid) {
      // unlock previously locked segment if any
      if (last_segment_locked < index_info.nsegments) {
        segment_io[last_segment_locked].lock.unlock();
      }
      // lock the current segment I am going to read
      segment_io[segmentid].lock.lock();
      last_segment_locked = segmentid;
    }
    if (!segment_io[segmentid].fin) {
      segment_io[segmentid].fin = new general_ifstream(
          index_info.segment_files[segmentid],
          false);
    }
    if (!segment_io[segmentid].fin->good()) continue;

    // seek to the block, skip the data
    segment_io[segmentid].fin->clear();
    segment_io[segmentid].fin->seekg(
        segments[segmentid].blocks[blockid].offset + sizeof(block_header), 
        std::ios_base::beg);

    if (segments[segmentid].blocks[blockid].flags & LZ4_COMPRESSION) {
      /*
       * Read into a temporary block before decompression into the 
       * caller's buffer
       */
      size_t blockbytes = segments[segmentid].blocks[blockid].length;
      segment_io[segmentid].compression_buffer.resize(blockbytes);
      char* tmp = segment_io[segmentid].compression_buffer.data();
      iolock.lock();
      segment_io[segmentid].fin->read(tmp, blockbytes);
      iolock.unlock();
      size_t retlen = index_info.block_size;
      retlen = LZ4_decompress_safe(tmp, c[i], blockbytes, retlen);
      ret[i] = retlen;
    } else {
      /*
       * No compression, direct read
       */
      size_t blockbytes = segments[segmentid].blocks[blockid].length;
      iolock.lock();
      segment_io[segmentid].fin->read(c[i], blockbytes);
      iolock.unlock();
      ret[i] = blockbytes;
    }
  }
  // unlock the segment
  if (last_segment_locked < index_info.nsegments) {
    segment_io[last_segment_locked].lock.unlock();
  }
  return ret;
}


void block_reader::load_segment_block_info(size_t segmentid) {
  // fast check
  ASSERT_LT(segmentid, index_info.nsegments);
  if (segments[segmentid].segment_loaded) return;
  // lock on segment information must be acquired
  std::lock_guard<mutex> lock(segments[segmentid].lock);
  // recheck
  if (segments[segmentid].segment_loaded) return;

  size_t filesize;
  size_t footer_size;

  try {
    if (!index_info.segment_files[segmentid].empty()) {
      // read the footer size
      general_ifstream fin(index_info.segment_files[segmentid], 
                           false); /* file must not be compressed 
                                      regardless of the filename */
      // jump to the footer
      filesize = fin.file_size();
      fin.seekg(filesize - 8, std::ios_base::beg);
      // read 8 bytes
      footer_size = 0;
      fin.read(reinterpret_cast<char*>(&footer_size), sizeof(footer_size));
      ASSERT_EQ(footer_size % sizeof(block_header), 0);

      // seek to the footer start
      fin.clear();
      fin.seekg(filesize - footer_size - 8, std::ios_base::beg);
      size_t numblocks = footer_size / sizeof(block_header);

      size_t currentoffset = 0;
      size_t start_row = segments[segmentid].start_row;
      for (size_t i = 0; i < numblocks; ++i) {
        block_header header;
        fin.read(reinterpret_cast<char*>(&header), sizeof(block_header));
        if (fin.eof() || fin.fail()) break;
        block_info block;
        block.offset = currentoffset; 
        block.length = header.num_bytes;
        block.start_row = start_row;
        block.num_elem = header.num_elements;
        block.flags = header.flags;
        segments[segmentid].blocks.push_back(block);
        currentoffset += sizeof(block_header) + header.num_bytes;
        start_row += header.num_elements;
      }
    }
  } catch (...) {
    // fail loading the segment file
    logstream(LOG_ERROR) << "Fail loading segment block info on segment " << segmentid << std::endl;
  }
  segments[segmentid].segment_loaded = true;
}

/**************************************************************************/
/*                                                                        */
/*                              block_writer                              */
/*                                                                        */
/**************************************************************************/

void block_writer::set_num_segments(size_t num_segments) {
  output_files.resize(num_segments);
  all_block_information.resize(num_segments);
  compression_buffer.resize(num_segments);
}

void block_writer::open_segment(size_t segmentid, std::string filename) {
  output_files[segmentid].reset(new general_ofstream(filename, 
                                                    /* must not compress! 
                                                     * We need the blocks!*/
                                                     false));
  if (output_files[segmentid]->fail()) {
    log_and_throw("Unable to open segment data file " + filename);
  }
}

void block_writer::write_block(size_t segmentid,
                               char* data,
                               size_t len,
                               size_t num_elements,
                               uint64_t flags) {
  v1_block_impl::block_header header;
  header.num_elements = num_elements;
  header.flags = flags;
  if (flags & LZ4_COMPRESSION) {
    /*
     * Compress into a temporary block before writing it
     */
    compression_buffer[segmentid].resize(LZ4_compressBound(len));
    char* tmp = compression_buffer[segmentid].data();
    size_t tmplen = compression_buffer[segmentid].size();
    tmplen = LZ4_compress(data, tmp, len);
    // write a block header
    header.num_bytes = tmplen;
    all_block_information[segmentid].push_back(header);
    // write the header
    output_files[segmentid]->write(reinterpret_cast<char*>(&header), 
                                   sizeof(v1_block_impl::block_header));
    output_files[segmentid]->write(tmp, tmplen);
  } else {
    /*
     * No compression, direct write
     */
    header.num_bytes = len;
    all_block_information[segmentid].push_back(header);
    // write the header
    output_files[segmentid]->write(reinterpret_cast<char*>(&header), 
                                   sizeof(v1_block_impl::block_header));
    output_files[segmentid]->write(data, len);
  }
}

void block_writer::close_segment(size_t segmentid) {
  emit_footer(segmentid);
  output_files[segmentid].reset();
}

void block_writer::emit_footer(size_t segmentid) {
  // prepare the footer
  // write out all the block headers
  for (size_t i = 0;i < all_block_information[segmentid].size(); ++i) {
    output_files[segmentid]->write(
        reinterpret_cast<char*>(&(all_block_information[segmentid][i])),
        sizeof(v1_block_impl::block_header));
  }
  // what is the length of the footer?
  uint64_t footer_size = 
      all_block_information[segmentid].size() * 
      sizeof(v1_block_impl::block_header);

  output_files[segmentid]->write(reinterpret_cast<char*>(&footer_size),
                                 sizeof(footer_size));
}

} // namespace v1_block_impl
} // namespace graphlab
