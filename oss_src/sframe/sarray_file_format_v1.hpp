/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_SFRAME_SARRAY_FILE_FORMAT_V1_HPP
#define GRAPHLAB_SFRAME_SARRAY_FILE_FORMAT_V1_HPP
#include <string>
#include <memory>
#include <typeinfo>
#include <map>
#include <parallel/mutex.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <timer/timer.hpp>
#include <logger/logger.hpp>
#include <sframe/sarray_file_format_interface.hpp>
#include <sframe/sarray_index_file.hpp>
#include <fileio/general_fstream.hpp>
#include <fileio/temp_files.hpp>
#include <serialization/serialization_includes.hpp>
#include <sframe/sarray_index_file.hpp>
#include <sframe/sarray_v1_block_manager.hpp>
#include <cppipc/server/cancel_ops.hpp>
namespace graphlab {
// anonymous namespace, local only
namespace {
/**
 * The default size of each block in the file
 */
static const size_t DEFAULT_BLOCK_SIZE = 512 * 1024 /*512K*/;
/**
 * The maximum number of blocks that can be maintained in a reader's cache
 */
static const size_t MAX_BLOCKS_IN_CACHE = 512;

/**
 * The maximum number of prefetch entries for read_segment()
 */
static const size_t MAX_ROWS_IN_CACHE_PER_READ_SEGMENT = 655360;
}

/**
 * This class implements the version 1 file format.
 * See below for the file format design and specification.
 *
 * ## version 1 file format design
 * Like the version 0 file format, the version 1 file format is designed to not
 * so \b simple, but extensible.
 *
 * An sarray on disk is held as a collection of files with a common prefix.
 * There is an index file, followed by a collection of data files.
 *
 * - Index File: [prefix].sidx
 * - Data Files: [prefix].0000 [prefix].0001 [prefix].0002 etc.
 *
 * ### Index Files
 * The index file has a suffix \c .sidx and contains basic information about
 * the data files. The format of the index file is the Microsoft INI format
 * http://en.wikipedia.org/wiki/INI_file, with the following sections and keys.
 * All sections and keys are required
 * 
 * \verbatim
 * [sarray]
 * ; The version of the file format. Required.
 * version=1
 * ; Number of segments in the array. Required
 * num_segments=16
 * ; The C++ mangled typeid name of the contents. Optional.
 * content_type=i 
 * ; The block size in bytes
 * block_size=512768
 *
 * [segment_sizes]
 * ;For each segment, the number of elements in the segment
 * ;There must be num_segments segments ranging from 0000 to num_segments-1
 * 0000=67
 * 0001=24
 * 0002=57
 *
 * \endverbatim
 *
 * ### Data Files
 * Data files are numbered with a four numeral (base 10) suffix. Numberings are
 * sequential beginning at 0000. Each data file corresponds to one segment.
 * Each data file is represented as a collection of blocks.
 * Each block begins with a 24 byte header:
 *   - 8 bytes - number of elements in this block
 *   - 8 bytes - number of bytes in the block (excluding this header)
 *   - 8 bytes - reserved for flags.
 * The remaining contents of each block is then no more than a sequential
 * serialization of objects of type T. 
 *
 * Each data file then has a variable length footer which is 
 *   - all the block headers repeated in sequential order.
 *   - 8 bytes  - integer containing the length of the above structure. 
 *                (i.e. length of the complete footer - 8)
 * The size of block on disk can be variable (due to compression). 
 */
template <typename T>
class sarray_format_reader_v1: public sarray_format_reader<T> {
 public:
  /// Default Constructor
  inline sarray_format_reader_v1() {}

  /**
   * Destructor. Also closes the sarray if open.
   */
  inline ~sarray_format_reader_v1() {
    close();
  }

  /// deleted copy constructor
  sarray_format_reader_v1(const sarray_format_reader_v1& other) = delete;

  /// deleted assignment
  sarray_format_reader_v1& operator=(const sarray_format_reader_v1& other) = delete;


  /**
   * Open has to be called before any of the other functions are called.
   * Throws a string exception if it is unable to open the index file, or if
   * there is a format error in the sarray index file.
   *
   * Will throw an exception if a file set is already open.
   */
  void open(index_file_information index) {
    index_file = "";
    ASSERT_MSG(!array_open, "sarray already open");
    index_info = index;
    initialize();
  }

  /**
   * Open has to be called before any of the other functions are called.
   * Throws a string exception if it is unable to open the index file, or if
   * there is a format error in the sarray index file.
   *
   * Will throw an exception if a file set is already open.
   */
  void open(std::string sidx_file) {
    index_file = sidx_file;
    ASSERT_MSG(!array_open, "sarray already open");
    index_info = read_index_file(index_file);
    initialize();
  }

  /**
   * Closes an sarray file set. No-op if the array is already closed.
   */
  void close() {
    // no-op is already closed
    if (!array_open) return;
    index_info = index_file_information();
    // mark array as closed
    array_open = false;
  }

  /**
   * Return the number of segments in the sarray
   */
  size_t num_segments() const {
    ASSERT_MSG(array_open, "sarray not open");
    return index_info.nsegments;
  }

  /**
   * Returns the number of elements in a given segment.
   */
  size_t segment_size(size_t segmentid) const {
    ASSERT_MSG(array_open, "sarray not open");
    ASSERT_LT(segmentid, index_info.nsegments);
    return index_info.segment_sizes[segmentid];
  }

  /**
   * Gets the contents of the index file information read from the index file
   */
  const index_file_information& get_index_info() const {
    return index_info;
  }

  /**
   * Returns the index_file of the array (the argument in \ref open)
   */
  std::string get_index_file() const {
    return index_file;
  }

  size_t read_rows(size_t row_start, 
                   size_t row_end, 
                   sframe_rows& out_obj) {
    return sarray_format_reader<T>::read_rows(row_start, row_end, out_obj);
  }

  /**
   * Reads a collection of rows, storing the result in out_obj.
   * This function is independent of the open_segment/read_segment/close_segment
   * functions, and can be called anytime. This function is also fully 
   * concurrent.
   * \param row_start First row to read
   * \param row_end one past the last row to read (i.e. EXCLUSIVE). row_end can
   o                be beyond the end of the array, in which case, 
   *                fewer rows will be read.
   * \param out_obj The output array
   * \returns Actual number of rows read. Return (size_t)(-1) on failure.
   *
   * \note This function is currently only optimized for "mostly" sequential 
   * reads. i.e. we are expecting read_rows(a, b), to be soon followed by
   * read_rows(b,c), etc.
   */
  size_t read_rows(size_t row_start, 
                   size_t row_end, 
                   std::vector<T>& out_obj) {
    out_obj.clear();
    cache_rows(row_start, row_end);
    while(row_start < row_end) {
      size_t cur_rows_read = add_rows_from_block(row_start, row_end, out_obj);
      if (cur_rows_read == (size_t)(-1)) break;
      row_start += cur_rows_read;
      if (cur_rows_read == 0) break;

      if(cppipc::must_cancel()) {
        throw(std::string("Cancelled by user."));
      }
    }
    return out_obj.size();
  }

 private:
  typedef v1_block_impl::block_reader::block_id block_id;
  bool array_open = false; // true if we are currently reading a segment

  std::string index_file;
  index_file_information index_info;
  v1_block_impl::block_reader block_reader;
  /*
   * Datastructures to manage read_rows
   */
  mutex block_cache_lock;
  /**
   * In memory cache of a block
   */
  struct block_cache_data {
    mutex lock;
    // the block address cached
    block_id block_address{0,0};
    // The block cache
    std::vector<char> buffer;
    // known / cached rows to buffer offset addresses
    std::map<size_t, size_t> row_to_offset;

    double last_access_time = 0.0;
  };
  std::map<block_id, std::shared_ptr<block_cache_data> > block_cache;
  timer current_time;


  /**
   * Initializes the internal data structures based on the information in the
   * index_info
   */
  void initialize() {
    // open the block reader
    block_reader.init(index_info);
    // array is now open for reading
    array_open = true;
  }


  /**
   * Caches all the blocks which includes the range of rows requested
   */
  void cache_rows(size_t row_start, size_t row_end) {
    std::vector<block_id> blocks_to_cache;
    block_id block_address = block_reader.block_containing_row(row_start);
    if (block_address.first == (size_t)(-1)) {
      log_and_throw("Unable to read row at " + std::to_string(row_start));
    }
    while(row_start < row_end) {
      block_id block_address = block_reader.block_containing_row(row_start);
      if (block_address.first == (size_t)(-1)) return;
      block_cache_lock.lock();
      size_t in_cache = block_cache.count(block_address);
      block_cache_lock.unlock();
      // if not in cache, we need to cache it
      if (!in_cache) {
        blocks_to_cache.push_back(block_address);
      }
      size_t first_row_in_block = block_reader.first_row_of_block(block_address);
      size_t rows_in_block = block_reader.num_elem_in_block(block_address);
      size_t last_row_in_block = first_row_in_block + rows_in_block;
      size_t last_row_to_read = std::min(row_end, last_row_in_block);
      row_start = last_row_to_read;
    }
    cache_blocks(blocks_to_cache);
  }

  /**
   * Caches a collection of blocks into the cache, and also returns a reference
   * to the blocks.
   */
  std::vector<std::shared_ptr<block_cache_data> > 
      cache_blocks(const std::vector<block_id>& blocks) {
    std::vector<std::shared_ptr<block_cache_data> > entries(blocks.size());
    if (blocks.size() == 0) return entries;
    std::vector<char*> buffers;
    // prepare the read_blocks call. Allocate the buffers, and the entries
    for(auto& entry: entries) {
      entry.reset(new block_cache_data);
      entry->buffer.resize(index_info.block_size);
      buffers.push_back(entry->buffer.data());
    }
    auto lens = block_reader.read_blocks(blocks, buffers);
    DASSERT_EQ(lens.size(), blocks.size());
    for (size_t i = 0; i < blocks.size(); ++i) {
      if (lens[i] == (size_t)(-1)) {
        throw("Failed to fetch block " + 
              std::to_string(blocks[i].first) + ":" + 
              std::to_string(blocks[i].second));
      } else {
        entries[i]->buffer.resize(lens[i]);
      }
      add_new_entry_to_cache(blocks[i], entries[i]);
    }
    return entries;
  }

  /**
   * Inserts a new cache entry into the cache
   */
  void add_new_entry_to_cache(block_id block_address,
                              std::shared_ptr<block_cache_data> entry) {
    // fill the start and end into the row_to_offset cache
    // this helps the look up later by setting left and right
    // boundaries.
    size_t first_row_in_block = block_reader.first_row_of_block(block_address);
    size_t rows_in_block = block_reader.num_elem_in_block(block_address);
    entry->row_to_offset[first_row_in_block] = 0;
    entry->row_to_offset[first_row_in_block + rows_in_block] = entry->buffer.size();
    entry->last_access_time = current_time.current_time();
    // acquire lock and stick it into the cache
    std::unique_lock<mutex> lock(block_cache_lock);
    block_cache[block_address] = entry;
  }

  /**
   * Retrieves a block either from the cache, or from the block reader
   */
  std::shared_ptr<block_cache_data> fetch_block(block_id block_address) {
    std::unique_lock<mutex> lock(block_cache_lock);
    if (block_cache.size() > MAX_BLOCKS_IN_CACHE) {
      lock.unlock();
      uncache_oldest();
      lock.lock();
    }
    auto iter = block_cache.find(block_address);
    if (iter == block_cache.end()) {
      // data is not in the cache!
      // fetch it from disk. unlock the lock first
      lock.unlock();
      auto entry = cache_blocks(std::vector<block_id>{block_address})[0];
      return entry;
    } else {
      iter->second->last_access_time = current_time.current_time();
      return iter->second;
    }
  }
  
  void uncache(v1_block_impl::block_reader::block_id block_address) {
    std::lock_guard<mutex> lock(block_cache_lock);
    block_cache.erase(block_address);
  }

  void uncache_oldest() {
    std::lock_guard<mutex> lock(block_cache_lock);
    // look for the oldest block
    while(block_cache.size() > MAX_BLOCKS_IN_CACHE) {
      auto oldest_iter = block_cache.begin();
      auto iter = block_cache.begin();
      while(iter != block_cache.end()) {
        if (iter->second->last_access_time < 
            oldest_iter->second->last_access_time) {
          oldest_iter = iter;
        }
        ++iter;
      }
      block_cache.erase(oldest_iter);
    }
  }
  /**
   * Internal function. Reads a collection of rows, \b appending the result in 
   * out_obj, but \b stopping at a block boundary.
   * This function is independent of the open_segment/read_segment/close_segment
   * functions, and can be called anytime. This function is also fully 
   * concurrent.
   * \param row_start First row to read
   * \param row_end one past the last row to read (i.e. EXCLUSIVE). row_end can
   *                be beyond the end of the array, in which case, 
   *                fewer rows will be read.
   * \param out_obj The output array
   * \returns Actual number of rows read. Return (size_t)(-1) on failure.
   */
  size_t add_rows_from_block(size_t row_start, 
                             size_t row_end,
                             std::vector<T>& out_obj) {
    // find the block
    block_id block_address = block_reader.block_containing_row(row_start);

    // failure. start row not found
    if (block_address.first == (size_t)(-1)) return (size_t)(-1);

    // get some basic information about the block that we will need
    size_t first_row_in_block = block_reader.first_row_of_block(block_address);
    size_t rows_in_block = block_reader.num_elem_in_block(block_address);
    size_t last_row_in_block = first_row_in_block + rows_in_block;

    // fetch the block
    std::shared_ptr<block_cache_data> block = fetch_block(block_address);
    std::unique_lock<mutex> guard(block->lock);
    auto iter = block->row_to_offset.lower_bound(row_start);
    // imposible
    ASSERT_TRUE(iter != block->row_to_offset.end());
    // lower_bound returns the first element >= to the row_start
    // if iter->first != row_start (i.e. greater)
    // we jump to the previous known position, so we can seek forward from there.
    if (iter->first > row_start) --iter;
    // read out the iterator
    size_t currow = iter->first;
    size_t curoffset = iter->second;
    // we can release the lock here. We are done with the lookup
    guard.unlock();
    iarchive iarc(block->buffer.data() + curoffset, 
                  block->buffer.size() - curoffset);
    // read all the rows we do not care about.
    T temp;
    while(currow < row_start) {
      iarc >> temp;
      ++currow;
    }
    size_t last_row_to_read = std::min(row_end, last_row_in_block);
    size_t rows_read = last_row_to_read - currow;
    while(currow < last_row_to_read) {
      iarc >> temp;
      out_obj.push_back(std::move(temp));
      ++currow;
    }
    
    if (row_end < last_row_in_block) {
      // add a cache of the position
      guard.lock();
      block->row_to_offset[row_end] = curoffset + iarc.off;
    } else {
      // block has been completely read. Uncache it
      uncache(block_address);
    }
    return rows_read;
  }

};


/**
 * The format v1 writer. See the format reader for details on the file format.
 */
template <typename T>
class sarray_format_writer_v1 {
 public:
  /// Default constructor
  sarray_format_writer_v1():array_open(false) { }

  /**
   * Destructor. Also closes the sarray if open.
   */
  ~sarray_format_writer_v1() {
    close();
  }

  /// deleted copy constructor
  sarray_format_writer_v1(const sarray_format_writer_v1& other) = delete;

  /// deleted assignment
  sarray_format_writer_v1& operator=(const sarray_format_writer_v1& other) = delete;


  /**
   * Open has to be called before any of the other functions are called.
   * Throws a string exception if it is unable to open the file set, or 
   * if the file set already exists.  Will also throw an exception if a file
   * set is already open. It will be created with a block size of 
   * DEFAULT_BLOCK_SIZE.
   *
   * \param sidx_file The sarray index file to write
   * \param segments_to_create The number of segments the sarray is split into
   */
  void open(std::string sidx_file, 
            size_t segments_to_create) {
    open(sidx_file, segments_to_create, DEFAULT_BLOCK_SIZE);
  }

  /**
   * Open has to be called before any of the other functions are called.
   * Throws a string exception if it is unable to open the file set, or 
   * if the file set already exists.  Will also throw an exception if a file
   * set is already open.
   *
   * \param sidx_file The sarray index file to write
   * \param segments_to_create The number of segments the sarray is split into
   * \param block_size The size of each block within the segment. Note that this
   * only affects the pre-compression size (i.e. it tries to size the serialized
   * block this block_size. But after writing to disk, the size may change).
   */
  void open(std::string sidx_file, 
            size_t segments_to_create,
            size_t block_size) {
    ASSERT_MSG(!array_open, "sarray already open");
    ASSERT_MSG(boost::algorithm::ends_with(sidx_file, ".sidx"),
               "Index file must end with .sidx");
    array_open = true;
    index_file = sidx_file;
    index_info = index_file_information();
    index_info.version = 1;
    index_info.nsegments = segments_to_create;
    index_info.segment_sizes.resize(index_info.nsegments, 0);
    index_info.segment_files.resize(index_info.nsegments);
    index_info.content_type = typeid(T).name();
    index_info.block_size = block_size;
    segment_data.resize(index_info.nsegments);
    last_block_size.resize(index_info.nsegments);
    writer.set_num_segments(index_info.nsegments);
  }

  /**
   * Closes an sarray file set. No-op if the array is already closed.
   * Also commits the index file.
   */
  void close() {
    // no-op is already closed
    if (!array_open) return;
    // close all writers
    for (size_t i = 0;i < segment_data.size(); ++i) {
      close_segment(i);
    }

    write_index_file();

    // done! 
    // clear all variables
    array_open = false;
    index_file = "";
    index_info = index_file_information();
    segment_data.clear();
  }

  /** 
   * Returns the number of parallel output segments
   * Throws an exception if the array is not open.
   */
  size_t num_segments() const {
    ASSERT_MSG(array_open, "sarray not open");
    return index_info.nsegments;
  }


  /** 
   * Returns the size of each block inside the SArray
   */
  size_t block_size() const {
    return index_info.block_size;
  }

  /**
   * Returns the number of elements written to a segment so far.
   * should throw an exception if the segment ID does not exist,
   */
  virtual size_t segment_size(size_t segmentid) const {
    return index_info.segment_sizes[segmentid];
  }

  /**
   * Makes a particular segment writable with \ref write_segment.
   * Should throw an exception if the segment is already open, or if
   * the segment ID does not exist. Each segment should only be open once.
   */
  void open_segment(size_t segmentid) {
    log_func_entry();
    ASSERT_MSG(array_open, "sarray not open");
    ASSERT_LT(segmentid, index_info.nsegments);
    ASSERT_MSG(!segment_data[segmentid], "Segment already open");

    std::string filename;
    // put it in the same location as the index file
    // generate a prefix for the file. if segmentid is 1, this generates 0001
    // if segmentid is 2 this generates 0002, etc.
    std::stringstream strm;
    strm << index_file.substr(0, index_file.length() - 5) << ".";
    strm.fill('0'); strm.width(4);
    strm << segmentid;
    filename = strm.str();
    logstream(LOG_DEBUG) << "Open segment " << segmentid
      << " for write on " << filename << std::endl;
    writer.open_segment(segmentid, filename);
    // update the index information
    index_info.segment_files[segmentid] = filename;
    // set up the in memory buffer
    segment_data[segmentid].reset(new oarchive);
    segment_data[segmentid]->expand_buf(index_info.block_size);
  }

  /**
   * Writes an object to the segment.
   * Should throw an exception if the segment is not opened with open_segment.
   */
  void write_segment(size_t segmentid, const T& t) {
    DASSERT_MSG(array_open, "sarray not open");
    DASSERT_LT(segmentid, index_info.nsegments);
    DASSERT_MSG(segment_data[segmentid], "Segment not open");
    size_t prevlen = segment_data[segmentid]->off;
    (*segment_data[segmentid]) << t;
    // have we exceeded the block size?
    if (segment_data[segmentid]->off > index_info.block_size) {
      // yup!, revert.
      segment_data[segmentid]->off = prevlen;
      // flush_block clears the last_block_size[segmentid]
      flush_block(segmentid);
      (*segment_data[segmentid]) << t;
    }
    ++last_block_size[segmentid];
  }

  void write_segment(size_t segmentid, T&& t) {
    DASSERT_MSG(array_open, "sarray not open");
    DASSERT_LT(segmentid, index_info.nsegments);
    DASSERT_MSG(segment_data[segmentid], "Segment not open");
    size_t prevlen = segment_data[segmentid]->off;
    (*segment_data[segmentid]) << t;
    // have we exceeded the block size?
    if (segment_data[segmentid]->off > index_info.block_size) {
      // yup!, revert.
      segment_data[segmentid]->off = prevlen;
      // flush_block clears the last_block_size[segmentid]
      flush_block(segmentid);
      (*segment_data[segmentid]) << t;
    }
    ++last_block_size[segmentid];
  }

  /** Closes a segment. 
   * After a segment is closed, writing to the segment will throw an error.
   */
  void close_segment(size_t segmentid) {
    if (segmentid < index_info.nsegments) {
      // if there is an output file on the segment, close it 
      if (segment_data[segmentid]) {
        flush_block(segmentid);
        writer.close_segment(segmentid);
        free(segment_data[segmentid]->buf);
        segment_data[segmentid].reset();
      }
    }
  }

  /**
   * Returns the index file of the sarray files are living on.
   * \see get_file_names()
   */
  std::string get_index_file() const {
    return index_file;
  }

  index_file_information& get_index_info() {
    return index_info;
  }

  /**
   * Flushes the index_file_information to disk
   */
  virtual void write_index_file() {
    graphlab::write_index_file(index_file, index_info);
  }

 private:
  bool array_open = false;
  std::string index_file;
  index_file_information index_info;
  // The serialization write buffers for each open segment
  // Stores in memory, the last block that has not been written.
  // When the block has been written, the archive is cleared.
  std::vector<std::unique_ptr<oarchive> > segment_data;

  // The number of elements written to the segment_data that has not been
  // flushed to disk
  std::vector<size_t> last_block_size;
  v1_block_impl::block_writer writer;

  /**
   * Flushes the current contents of segment_data[segment_id] to disk
   * as a block.
   */
  void flush_block(size_t segmentid) {
    // if there is no data to write, skip
    if (last_block_size[segmentid] == 0) return;
    writer.write_block(segmentid,
                segment_data[segmentid]->buf,
                segment_data[segmentid]->off,
                last_block_size[segmentid], 
                v1_block_impl::LZ4_COMPRESSION /* flags */);
    // Reset all buffers so that the next block can be started
    // increment the data counter
    index_info.segment_sizes[segmentid] += last_block_size[segmentid];
    // reset the serialization buffer
    segment_data[segmentid]->off = 0;
    // reset the data counter
    last_block_size[segmentid] = 0;
  }
};
} // namespace graphlab
#endif
