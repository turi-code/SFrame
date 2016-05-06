#include <string>
#include <vector>
#include <map>
#include <algorithm>
#include <timer/timer.hpp>
#include <sframe/sarray.hpp>
#include <sframe/sframe.hpp>
#include <sframe/sframe_constants.hpp>
#include <sframe/sframe_config.hpp>
#include <sframe/sarray_v2_block_manager.hpp>
#include <flexible_type/flexible_type.hpp>
#include <sframe_query_engine/operators/project.hpp>
#include <sframe_query_engine/operators/union.hpp>
#include <sframe_query_engine/operators/sframe_source.hpp>
#include <sframe_query_engine/operators/range.hpp>
#include <sframe_query_engine/algorithm/sort.hpp>
#include <sframe_query_engine/planning/planner.hpp>
namespace graphlab {
namespace query_eval {
using sframe_config::SFRAME_SORT_BUFFER_SIZE;
using sframe_config::SFRAME_READ_BATCH_SIZE;
/**
* This returns the number of bytes after LZ4 decode needed for each column.
*
* This is used as a proxy for the size of the column. It does not represent
* at all, the actual size of the column in memory, but is a reasonable proxy.
*
* For instance, integers can be compressed to as low as 1 bit per integer,
* while in memory it will require 16 bytes.
*
* However, for complex values such as dicts, arrays, lists, this should be
* closer to the true in memory size; probably within a factor of 2 or 4.
* \param values The sframe
* \return a list of length ncolumns. Element i is the number of bytes on disk
*         used to store the column without LZ4 encoding.
*/
static std::vector<size_t> num_bytes_per_column(sframe& values) {
  std::vector<size_t> column_num_bytes(values.num_columns(), 0);
  auto& block_manager = v2_block_impl::block_manager::get_instance();

  // for each column
  for (size_t i = 0;i < values.num_columns(); ++i) {
    auto cur_column = values.select_column(i);
    auto column_index = cur_column->get_index_info();
    // for each segment in the column
    for(auto segment_file: column_index.segment_files) {
      auto segment_address = block_manager.open_column(segment_file);
      auto num_blocks_in_current_segment =
          block_manager.num_blocks_in_column(segment_address);
      // for each block in the segment
      for (size_t block_number = 0;
           block_number < num_blocks_in_current_segment;
           ++block_number) {
        v2_block_impl::block_address block_address{
                                std::get<0>(segment_address),
                                std::get<1>(segment_address),
                                block_number};
        column_num_bytes[i] += block_manager.get_block_info(block_address).block_size;
      }
      block_manager.close_column(segment_address);
    }
  }
  return column_num_bytes;
}

static std::vector<size_t> column_row_boundaries(std::shared_ptr<sarray<flexible_type>> cur_column) {
  std::vector<size_t> row_boundaries;
  auto column_index = cur_column->get_index_info();
  // for each segment in the column
  size_t row_number = 0;
  row_boundaries.push_back(row_number);
  auto& block_manager = v2_block_impl::block_manager::get_instance();
  for(auto segment_file: column_index.segment_files) {
    auto segment_address = block_manager.open_column(segment_file);
    auto num_blocks_in_current_segment =
        block_manager.num_blocks_in_column(segment_address);
    // for each block in the segment
    for (size_t block_number = 0;
         block_number < num_blocks_in_current_segment;
         ++block_number) {
      v2_block_impl::block_address block_address{
                              std::get<0>(segment_address),
                              std::get<1>(segment_address),
                              block_number};
      size_t nrows = block_manager.get_block_info(block_address).num_elem;
      row_number += nrows;
      row_boundaries.push_back(row_number);
    }
    block_manager.close_column(segment_address);
  }
  return row_boundaries;
}
/**
 * Given the storage requirements of a column (via num_bytes_per_column),
 * and its type, return an estimate of the number of bytes of memory required
 * per value.
 */
static size_t column_bytes_per_value_estimate(size_t column_num_bytes,
                                              size_t num_rows,
                                              flex_type_enum column_type) {
  // initial estimate
  size_t bytes_per_value = (column_num_bytes + num_rows - 1) / num_rows;
  // fix up the estimate based on the type
  switch(column_type) {
   case flex_type_enum::INTEGER:
   case flex_type_enum::FLOAT:
   case flex_type_enum::DATETIME:
     // these are stored entirely within the flexible_type 
     bytes_per_value = sizeof(flexible_type);
     break;
   case flex_type_enum::STRING:
     // these incur some constant
     bytes_per_value += (sizeof(flexible_type) + sizeof(flex_string));
     break;
   case flex_type_enum::VECTOR:
     // these incur some constant
     bytes_per_value += (sizeof(flexible_type) + sizeof(flex_vec));
     break;
   default:
     // everything else is complicated to estimate
     // so we just scale it up by a slack factor of 2
     bytes_per_value = bytes_per_value * 2 + sizeof(flexible_type);
     break;
  }
  return bytes_per_value;
}

/**
 * A subroutine of ec_permute.
 *
 * Scatters the input into a collection of buckets using the forward_map.
 * The forward_map must be an SArray of the same length as the input and contain
 * every integer from 0 to len-1.
 *
 * Returns an sframe. The last column of the sframe is the per-bucket 
 * forward map.
 */
static sframe ec_scatter_partitions(sframe input,
                                    size_t rows_per_bucket,
                                    const std::vector<bool>& indirect_column,
                                    std::shared_ptr<sarray<flexible_type> > forward_map) {
  //  - For each (c,r,v) in data:
  //    Write (c,v) to bucket `bucket of forward_map(r)`
  //  - For each (c,r,v) in forward_map:
  //        Write (c,v) to bucket `bucket of forward_map(r)`
  logstream(LOG_INFO) << "input size " << input.size() << std::endl;
  logstream(LOG_INFO) << "forward map size " << forward_map->size() << std::endl;
  input = input.add_column(forward_map);
  auto num_buckets = (input.size() + rows_per_bucket - 1)/ rows_per_bucket;
  sframe output;
  auto out_column_types = input.column_types();
  for (size_t i = 0;i < out_column_types.size(); ++i) {
    if (indirect_column[i]) out_column_types[i] = flex_type_enum::INTEGER;
  }
  // the last 'bucket' column.
  output.open_for_write(input.column_names(), out_column_types, "", num_buckets);
  auto writer = output.get_internal_writer();

  // prepare all the readers
  std::vector<std::shared_ptr<sarray<flexible_type>::reader_type>> readers(input.num_columns());
  for (size_t i = 0;i < input.num_columns(); ++i) {
    auto cur_column = input.select_column(i);
    readers[i] = std::shared_ptr<sarray<flexible_type>::reader_type>(cur_column->get_reader());
  }
  // now. the challenge here is that the natural order of the sframe is not 
  // necessarily good for forward map lookups. The forward map lookup has to
  // *insanely* fast. Instead we are going to do it this way:
  //  - Use the SFRAME_SORT_BUFFER_SIZE to estimate how much forward map 
  //  we can keep in memory at any one time. Then in parallel over 
  //  columns of the sframe.
  size_t max_forward_map_in_memory = SFRAME_SORT_BUFFER_SIZE / sizeof(flexible_type);
  auto forward_map_reader = forward_map->get_reader();
  std::vector<flexible_type> forward_map_buffer;
  logstream(LOG_INFO) << "Beginning Scatter"  << std::endl;
  logstream(LOG_INFO) << "Maximum forward map in memory " 
                      << max_forward_map_in_memory 
                      << std::endl;


  for (size_t forward_map_start = 0; 
       forward_map_start < forward_map->size(); 
       forward_map_start += max_forward_map_in_memory) {
    size_t forward_map_end = std::min(forward_map_start + max_forward_map_in_memory, 
                                      forward_map->size());
    logstream(LOG_INFO) << "Processing rows " 
                        << forward_map_start 
                        << " to " << forward_map_end << std::endl;
    forward_map_buffer.resize(forward_map_end - forward_map_start);
    forward_map_reader->read_rows(forward_map_start, forward_map_end, 
                                  forward_map_buffer);

    // now in parallel over columns.
    atomic<size_t> col_number = 0;
    in_parallel([&](size_t unused, size_t unused2) {
                    size_t column_id = col_number.inc_ret_last();
                    if (column_id >= input.num_columns()) return;
                    std::vector<size_t> boundaries = 
                        column_row_boundaries(input.select_column(column_id));
                    std::vector<flexible_type> buffer;

                    for (size_t i = 0;
                         i < boundaries.size() - 1;
                         ++i) {
                      size_t row = boundaries[i];
                      size_t row_end = boundaries[i + 1];
                      if (row_end < forward_map_start) continue;
                      row = std::max(row, forward_map_start);
                      row_end = std::min(row_end, forward_map_end);
                      // we have ended on the right hand side. quit
                      if (row >= row_end) break;
                      readers[column_id]->read_rows(row, row_end, buffer);
                      // scatter
                      for (size_t i= 0;i < buffer.size(); ++i) {
                        size_t actual_row = row + i;
                        size_t output_row = 
                            forward_map_buffer[actual_row - forward_map_start].get<flex_int>();
                        size_t output_segment = output_row / rows_per_bucket;
                        if (output_segment >= num_buckets) output_segment = num_buckets - 1;
                        writer->write_segment(column_id, output_segment, buffer[i]);
                      }
                    }
                 });
  }
  output.close();
  return output;
}

/**
 * A subroutine of ec_permute.
 *
 * Scatters the input into a collection of buckets using the last 
 * column of the input as the forward_map.
 *
 * The forward_map must be an SArray of the same length as the input and contain
 * every integer from 0 to len-1.
 *
 * Returns the permuted sframe without the forward map.
 */
sframe ec_permute_partitions(sframe input,
                             sframe& prepartitioned_input,
                             size_t rows_per_bucket,
                             std::vector<size_t> column_bytes_per_value,
                             std::vector<bool> indirect_column) {
//     For each Bucket b:
//         Allocate Output vector of (Length of bucket) * (#columns)
//         Let S be the starting index of bucket b (i.e. b*N/k)
//         Let T be the ending index of bucket b (i.e. (b+1)*N/k)
//         Load forward_map[S:T] into memory
//         For each (c,r,v) in bucket b
//             Output[forward_map(r) - S][c] = v
//         Dump Output to an SFrame
//
  logstream(LOG_INFO) << "Final permute" << std::endl;
  ASSERT_GE(input.num_columns(), 1);
  ASSERT_EQ(input.num_columns(), prepartitioned_input.num_columns() + 1);
  auto num_input_columns = input.num_columns() - 1; // last column is the forward map
  auto num_buckets = (input.size() + rows_per_bucket - 1)/ rows_per_bucket;

  // prepare all the input readers
  std::vector<std::shared_ptr<sarray<flexible_type>::reader_type>> readers(num_input_columns);
  for (size_t i = 0;i < num_input_columns; ++i) {
    auto cur_column = input.select_column(i);
    const auto column_index = cur_column->get_index_info();
    ASSERT_EQ(column_index.segment_files.size(), num_buckets);
    readers[i] = std::shared_ptr<sarray<flexible_type>::reader_type>(cur_column->get_reader());
  }
  auto& block_manager = v2_block_impl::block_manager::get_instance();


  // prepare the output
  sframe output;
  output.open_for_write(prepartitioned_input.column_names(),
                        prepartitioned_input.column_types(), "", num_buckets);
  auto writer = output.get_internal_writer();


  auto forward_map_reader = input.select_column(num_input_columns)->get_reader();
  size_t MAX_SORT_BUFFER = SFRAME_SORT_BUFFER_SIZE / thread::cpu_count(); 

  atomic<size_t> atomic_bucket_id = 0;
  // for each bucket
  in_parallel([&](size_t unused, size_t unused2) {
    size_t bucket_id = atomic_bucket_id.inc_ret_last();
    if (bucket_id >= num_buckets) return;
    size_t row_start = bucket_id * rows_per_bucket;
    size_t row_end = std::min(input.size(), row_start + rows_per_bucket);
    size_t num_rows = row_end - row_start;

    logstream(LOG_INFO) << "Processing bucket " << bucket_id << std::endl;
    // pull in the forward map
    std::vector<flexible_type> forward_map_buffer;
    forward_map_buffer.resize(num_rows);
    forward_map_reader->read_rows(row_start, row_end, forward_map_buffer);


     size_t col_start = 0;
     while(col_start < num_input_columns) {
       size_t col_end = col_start + 1;
       size_t memory_estimate = column_bytes_per_value[col_start] * num_rows;
       while(memory_estimate < MAX_SORT_BUFFER && 
             col_end < num_input_columns) {
         size_t next_col_memory_estimate = column_bytes_per_value[col_end] * num_rows;
         // if we can fit the next column in memory. good. Otherwise break;
         if (next_col_memory_estimate + memory_estimate < MAX_SORT_BUFFER) {
           memory_estimate += next_col_memory_estimate;
           ++col_end;
         } else {
           break;
         }
       }
 
       logstream(LOG_INFO) << "  Columns " << col_start << " to " << col_end << std::endl;

       std::vector<std::vector<flexible_type> > 
           permute_buffer(col_end - col_start,
                          std::vector<flexible_type>(num_rows)); // buffer after permutation

      // this is the order I am going to read the blocks in this bucket
      std::vector<v2_block_impl::block_address> block_read_order;
      std::map<v2_block_impl::column_address, size_t> column_id_from_column_address;
      std::vector<size_t> cur_row_number(col_end - col_start, 0);
      timer ti;
      for (size_t column_id = col_start; column_id < col_end ; ++column_id) {
        // look in the segment file and list all the blocks
        const auto column_index = input.select_column(column_id)->get_index_info();
        auto segment_address = block_manager.open_column(column_index.segment_files[bucket_id]);
        column_id_from_column_address[segment_address] = column_id;

        auto num_blocks_in_current_segment =
            block_manager.num_blocks_in_column(segment_address);
        // for each block in the segment
        for (size_t block_number = 0;
             block_number < num_blocks_in_current_segment;
             ++block_number) {
          block_read_order.push_back(v2_block_impl::block_address{
                                     std::get<0>(segment_address),
                                     std::get<1>(segment_address),
                                     block_number});
        }
      }
      // now sort the block_read_order by the block info offset
      std::sort(block_read_order.begin(), block_read_order.end(),
                [&](const v2_block_impl::block_address& left, 
                    const v2_block_impl::block_address& right) {
                  return block_manager.get_block_info(left).offset < 
                              block_manager.get_block_info(right).offset;
                });
      ti.start();
      // good. now we fetch the blocks in that order.
      std::vector<flexible_type> buffer;
      for (auto& block: block_read_order) {
        block_manager.read_typed_block(block, buffer);
        v2_block_impl::column_address col_address{std::get<0>(block), std::get<1>(block)};
        size_t column_id = column_id_from_column_address.at(col_address);

        size_t& row_number = cur_row_number[column_id - col_start];
        for (size_t i = 0; i < buffer.size(); ++i) {
          DASSERT_LT(row_number, forward_map_buffer.size());
          DASSERT_GE(forward_map_buffer[row_number], row_start);
          DASSERT_LT(forward_map_buffer[row_number], row_end);
          size_t target = forward_map_buffer[row_number] - row_start;
          permute_buffer[column_id - col_start][target] = std::move(buffer[i]);
          ++row_number;
        }
      }

      for(auto kv: column_id_from_column_address) block_manager.close_column(kv.first);
      logstream(LOG_INFO) << "Permute buffer fill in " << ti.current_time() << std::endl;
      ti.start();
      // write the permute buffer.
      for (size_t column_id = col_start; column_id < col_end ; ++column_id) {
        writer->write_column(column_id, bucket_id, std::move(permute_buffer[column_id - col_start]));
      }
      logstream(LOG_INFO) << "write columns in " << ti.current_time() << std::endl;
      ti.start();
      writer->flush_segment(bucket_id);
      logstream(LOG_INFO) << bucket_id << " flush in " << ti.current_time() << std::endl;
      col_start = col_end;
    }
  });

  output.close();
  return output;
}

/**
 * Permutes an sframe.
 * forward_map must be an SArray of the same length as the values_sframe,
 * containing every integer in the range 0 to len-1. Row i of the input sframe
 * is moved to row forward_map[i] of the output sframe.
 * The result is an SFrame of the same size as the input sframe, but with
 * its elements permuted.
 */
sframe permute_sframe(sframe &values_sframe,
                      std::shared_ptr<sarray<flexible_type> > forward_map) {
  auto num_rows = values_sframe.size();
  auto value_column_names = values_sframe.column_names();
  auto value_column_types = values_sframe.column_types();
  auto num_value_columns = values_sframe.num_columns();
  timer ti;
  // column_bytes_per_value: The average number of bytes of memory required for
  //                         a value in each columns
  // indirect_column : If true, we write a row number in scatter, and pick it
  //                   up again later.
  std::__1::vector<size_t> column_bytes_per_value(num_value_columns, 0);
  std::__1::vector<bool> indirect_column(num_value_columns, false);
  size_t num_buckets = 0;
  {
    // First lets get an estimate of the column sizes and we use that
    // to estimate the number of buckets needed.
    std::__1::vector<size_t> column_num_bytes = num_bytes_per_column(values_sframe);
    for (size_t i = 0;i < column_bytes_per_value.size();++i) {
      column_bytes_per_value[i] =
              column_bytes_per_value_estimate(column_num_bytes[i],
                                              num_rows,
                                              value_column_types[i]);
      // if bytes_per_value exceeds 256K, we use the indirect write.
      logstream(LOG_INFO) << "Est. bytes per value for column "
                          << value_column_names[i] << ": "
                          << column_bytes_per_value[i] << std::__1::endl;
      if (column_bytes_per_value[i] > 256 * 1024) {
        indirect_column[i] = true;
        column_bytes_per_value[i] = sizeof(flexible_type);
        logstream(LOG_INFO) << "Using indirect access for column "
                            << value_column_names[i] << std::__1::endl;
      }
      // update the number of bytes for the whole column
      column_num_bytes[i] = column_bytes_per_value[i] * num_rows;
    }


    ASSERT_GT(column_num_bytes.size(), 0);
    size_t max_column_num_bytes = *max_element(column_num_bytes.begin(),
                                               column_num_bytes.end());
    // maximum size of column / sort buffer size. round up
    // at least 1 bucket
    size_t HALF_SORT_BUFFER = SFRAME_SORT_BUFFER_SIZE / 2;
    num_buckets = (max_column_num_bytes + HALF_SORT_BUFFER - 1) / HALF_SORT_BUFFER;
    num_buckets = std::__1::max<size_t>(1, num_buckets);
    num_buckets *= thread::cpu_count();

    logstream(LOG_INFO) << "Generating " << num_buckets << " buckets" << std::__1::endl;

    size_t max_column_bytes_per_value = *max_element(column_bytes_per_value.begin(),
                                                     column_bytes_per_value.end());
    /*
     * There is a theoretical maximum number of rows we can sort, given
     * max_column_bytes_per_value. We can contain a maximum of
     * SFRAME_SORT_BUFFER_SIZE / max_column_bytes_per_value values per segment, and
     * we can only construct SFRAME_SORT_MAX_SEGMENTS number of segments.
     */
    size_t max_sort_rows =
            ((HALF_SORT_BUFFER * SFRAME_SORT_MAX_SEGMENTS) / max_column_bytes_per_value);
    logstream(LOG_INFO) << "Maximum sort rows: " << max_sort_rows << std::__1::endl;
    if (num_rows > max_sort_rows) {
      logstream(LOG_WARNING)
        << "With the current configuration of SFRAME_SORT_BUFFER_SIZE "
        << "and SFRAME_SORT_MAX_SEGMENTS "
        << "we can sort an SFrame of up to " << max_sort_rows << " elements\n"
        << "The size of the current SFrame exceeds this length. We will proceed anyway "
        << "If this fails, either of these constants need to be increased.\n"
        << "SFRAME_SORT_MAX_SEGMENTS can be increased by increasing the number of n"
        << "file handles via ulimit -n\n"
        << "SFRAME_SORT_BUFFER_SIZE can be increased with gl.set_runtime_config()"
        << std::__1::endl;
    }
  }

  //
  // Pivot Generation
  // ----------------
  // - Now we have a forward map, we can get exact buckets. Of N/K
  // length. I.e. row r is written to bucket `Floor(K \ forward_map(r) / N)`
  // ok. how many columns / rows can we fit in memory?
  // The limiter is going to be the largest column.
  //
  // Here we create bucket_start[i] and bucket_end[i] for each bucket
  std::__1::vector<size_t> bucket_start(num_buckets, 0);
  // due rows_per_bucket being an integer, a degree of imbalance
  // (up to num_buckets) is expected. That's fine.
  size_t rows_per_bucket = size_t(num_rows) / num_buckets;
  logstream(LOG_INFO) << "Rows per bucket: " << rows_per_bucket << std::__1::endl;
  for (size_t i = 0;i < num_buckets;++i) {
    bucket_start[i] = i * rows_per_bucket;
  }

  ti.start();
  logstream(LOG_INFO) << "Beginning scatter " << std::__1::endl;
  // Scatter
  // -------
  //  - For each (c,r,v) in data:
  //    Write (c,v) to bucket `Floor(K \ forward_map(r) / N)`
  sframe scatter_sframe = ec_scatter_partitions(values_sframe,
                                                rows_per_bucket,
                                                indirect_column,
                                                forward_map);
  logstream(LOG_INFO) << "Scatter finished in " << ti.current_time() << std::__1::endl;

  sframe sorted_values_sframe = ec_permute_partitions(scatter_sframe,
                                                      values_sframe,
                                                      rows_per_bucket,
                                                      column_bytes_per_value,
                                                      indirect_column);
  return sorted_values_sframe;
}




std::shared_ptr<sframe> ec_sort(
    std::shared_ptr<planner_node> sframe_planner_node,
    const std::vector<std::string> column_names,
    const std::vector<size_t>& key_column_indices,
    const std::vector<bool>& sort_orders) {

  // prep some standard metadata
  //  - num_columns
  //  - num_rows
  //  - key column information
  //      - key_column_names
  //      - key_column_indices
  //      - key_column_indices_set
  //      - key_columns
  //      - num_key_columns
  //  - value column information
  //      - value_column_names
  //      - value_column_indices
  //      - value_column_indices_set
  //      - value_columns
  //      - value_column_types
  //
  size_t num_columns = column_names.size();
  size_t num_rows = infer_planner_node_length(sframe_planner_node);
  // fast path for 0 rows.
  if (num_rows == 0) {
    return std::make_shared<sframe>(planner().materialize(sframe_planner_node));
  }
  // fast path for no value columns
  if (key_column_indices.size() == column_names.size()) {
    return sort(sframe_planner_node, column_names, 
                key_column_indices, sort_orders);
  }
  
  // key columns
  auto key_columns = op_project::make_planner_node(sframe_planner_node, key_column_indices);
  std::vector<std::string> key_column_names;
  std::for_each(key_column_indices.begin(), key_column_indices.end(),
                 [&](size_t i) {
                   key_column_names.push_back(column_names[i]);
                 });
  std::set<size_t> key_column_indices_set(key_column_indices.begin(), 
                                          key_column_indices.end());
  size_t num_key_columns = key_column_indices.size();

  // value columns
  std::vector<std::string> value_column_names;
  std::vector<size_t> value_column_indices;
  for (size_t i = 0 ;i < num_columns; ++i) {
    if (key_column_indices_set.count(i) == 0) value_column_indices.push_back(i);
  }
  auto value_columns = 
      op_project::make_planner_node(sframe_planner_node, value_column_indices);
  std::for_each(value_column_indices.begin(), value_column_indices.end(),
                 [&](size_t i) {
                   value_column_names.push_back(column_names[i]);
                 });
  std::set<size_t> value_column_indices_set(value_column_indices.begin(), 
                                            value_column_indices.end());
  std::vector<flex_type_enum> value_column_types = infer_planner_node_type(value_columns);



  // Forward Map Generation
  // ----------------------
  // 
  // - A set of row numbers are added to the key columns, and the key
  // columns are sorted. And then dropped. This gives the inverse map.
  // (i.e. x[i] = j implies output row i is read from input row j)
  // - Row numbers are added again, and its sorted again by the first set
  // of row numbers. This gives the forward map (i.e. y[i] = j implies
  // input row i is written to output row j)
  // - (In SFrame pseudocode:
  // 
  //     B = A[['key']].add_row_number('r1').sort('key')
  //     inverse_map = B['r1'] # we don't need this
  //     C = B.add_row_number('r2').sort('r1')
  //     foward_map = C['r2']

  std::shared_ptr<sarray<flexible_type> > forward_map;
  sframe sorted_key_columns;
  timer ti;
  ti.start();
  logstream(LOG_INFO) << "Creating forward map" << std::endl;

  {
    std::vector<std::string> forward_map_sort1_columns;
    // create new column names. Row number is first column. 'r1'
    forward_map_sort1_columns.push_back("r1");
    std::copy(key_column_names.begin(), key_column_names.end(),
              std::back_insert_iterator<std::vector<std::string>>(forward_map_sort1_columns));
    // all the key indices are all the columns
    std::vector<size_t> forward_map_sort1_column_indices;
    for (size_t i = 1; i <= key_column_names.size(); ++i) {
      forward_map_sort1_column_indices.push_back(i);
    }
    auto B = sort(op_union::make_planner_node(
                      op_range::make_planner_node(0, num_rows), key_columns),
                  forward_map_sort1_columns,
                  forward_map_sort1_column_indices,
                  sort_orders);
    logstream(LOG_INFO) << "sort finished in " << ti.current_time() << std::endl;
    ti.start();
    auto inverse_map = op_project::make_planner_node(
        op_sframe_source::make_planner_node(*B), {0});

    // remember the sorted column names. We are going to need it
    // when constructing the final SFrame
    sorted_key_columns = planner().materialize(
        op_project::make_planner_node(
            op_sframe_source::make_planner_node(*B), 
            forward_map_sort1_column_indices));
    ASSERT_EQ(sorted_key_columns.num_columns(), num_key_columns);

    for (size_t i = 0;i < sorted_key_columns.num_columns(); ++i) {
      sorted_key_columns.set_column_name(i, key_column_names[i]);
    }

    // now generate the forward map
    ti.start();
    auto materialized_inverse_map = planner().materialize(inverse_map).select_column(0);
    sframe incremental_array = planner().materialize(op_range::make_planner_node(0, num_rows));

    forward_map = permute_sframe(incremental_array, materialized_inverse_map).select_column(0);
    logstream(LOG_INFO) << "forward map generation finished in " << ti.current_time() << std::endl;
  }


  // Here we generate some more variables we need for the rest of the process
  // values_sframe: The raw sframe containing just the value columns
  sframe values_sframe = planner().materialize(value_columns);
  for (size_t i = 0;i < values_sframe.num_columns(); ++i) {
    values_sframe.set_column_name(i, value_column_names[i]);
  }

  sframe sorted_values_sframe = permute_sframe(values_sframe, forward_map);

  sframe final_sframe = sorted_key_columns;
  for (size_t i = 0;i < sorted_values_sframe.num_columns(); ++i) {
    final_sframe = final_sframe.add_column(sorted_values_sframe.select_column(i),
                                           sorted_values_sframe.column_name(i));
  }
  return std::make_shared<sframe>(final_sframe);
}

} // namespace query_eval
} // namespace graphlab
