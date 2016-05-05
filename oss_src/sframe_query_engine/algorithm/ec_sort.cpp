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
    }
  }
  return column_num_bytes;
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

static sframe ec_scatter_partitions(sframe& input,
                                    size_t rows_per_bucket,
                                    size_t num_buckets,
                                    const std::vector<bool>& indirect_column,
                                    std::shared_ptr<sarray<flexible_type> > forward_map) {
  //  - For each (c,r,v) in data:
  //    Write (c,v) to bucket `bucket of [forward_map(r)]`

  sframe output;
  auto out_column_types = input.column_types();
  for (size_t i = 0;i < out_column_types.size(); ++i) {
    if (indirect_column[i]) out_column_types[i] = flex_type_enum::INTEGER;
  }
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


  for (size_t forward_map_start = 0; 
       forward_map_start < forward_map->size(); 
       forward_map_start += max_forward_map_in_memory) {
    size_t forward_map_end = std::min(forward_map_start + max_forward_map_in_memory, 
                                      forward_map->size());
    forward_map_buffer.resize(forward_map_end - forward_map_start);
    forward_map_reader->read_rows(forward_map_start, forward_map_end, 
                                  forward_map_buffer);

    // now in parallel over columns.
    parallel_for(0, input.num_columns(), [&](size_t column_id) {
                    std::vector<flexible_type> buffer;
                    for (size_t row = forward_map_start;
                         row < forward_map_end;
                         row += SFRAME_READ_BATCH_SIZE) {
                      size_t row_end = std::min(row + SFRAME_READ_BATCH_SIZE,
                                              forward_map_end);
                      readers[column_id]->read_rows(row, row_end, buffer);
                      // scatter
                      for (size_t i= 0;i < buffer.size(); ++i) {
                        size_t actual_row = row + i;
                        size_t output_row = forward_map_buffer[actual_row - forward_map_start];
                        size_t output_segment = output_row / num_buckets;
                        if (output_segment >= num_buckets) output_segment = num_buckets - 1;
                        writer->write_segment(column_id, output_segment, std::move(buffer[i]));
                      }
                    }
                 });
  }
  output.close();
  return output;
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
  //      - num_value_columns
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
  size_t num_value_columns = value_column_indices.size();



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
    logstream(LOG_INFO) << "First sort finished in " << ti.current_time() << std::endl;
    ti.start();
    auto inverse_map = op_project::make_planner_node(
        op_sframe_source::make_planner_node(*B), {0});

    sorted_key_columns = planner().materialize(
        op_project::make_planner_node(
            op_sframe_source::make_planner_node(*B), 
            forward_map_sort1_column_indices));

    auto C = sort(op_union::make_planner_node(
                op_range::make_planner_node(0, num_rows), inverse_map),
                {"r2","r1"},
                {1},
                {true});
    logstream(LOG_INFO) << "Second sort finished in " << ti.current_time() << std::endl;
    ti.start();
    forward_map = 
        planner().materialize(op_project::make_planner_node(
                op_sframe_source::make_planner_node(*C), {0})).select_column(0);
  }


  // Here we generate some more variables we need for the rest of the process
  // values_sframe: The raw sframe containing just the value columns
  // column_bytes_per_value: The average number of bytes of memory required for 
  //                         a value in each columns
  // indirect_column : If true, we write a row number in scatter, and pick it 
  //                   up again later.
  sframe values_sframe = planner().materialize(value_columns);
  std::vector<size_t> column_bytes_per_value(num_value_columns, 0);
  std::vector<bool> indirect_column(num_value_columns, false);
  size_t num_buckets = 0;
  { 
    // First lets get an estimate of the column sizes and we use that
    // to estimate the number of buckets needed.
    std::vector<size_t> column_num_bytes = num_bytes_per_column(values_sframe);
    for (size_t i = 0; i < column_bytes_per_value.size(); ++i) {
      column_bytes_per_value[i] = 
          column_bytes_per_value_estimate(column_num_bytes[i],
                                          num_rows,
                                          value_column_types[i]);
      // if bytes_per_value exceeds 256K, we use the indirect write.
      logstream(LOG_INFO) << "Est. bytes per value for column " 
                          << value_column_names[i] << ": " 
                          << column_bytes_per_value[i] << std::endl;
      if (column_bytes_per_value[i] > 256*1024) {
        indirect_column[i] = true;
        column_bytes_per_value[i] = sizeof(flexible_type);
        logstream(LOG_INFO) << "Using indirect access for column " 
                            << value_column_names[i] << std::endl;
      }
      // update the number of bytes for the whole column
      column_num_bytes[i] = column_bytes_per_value[i] * num_rows;
    }


    ASSERT_GT(column_num_bytes.size(), 0);
    size_t max_column_num_bytes = *std::max_element(column_num_bytes.begin(),
                                                    column_num_bytes.end());
    // maximum size of column / sort buffer size. round up
    // at least 1 bucket
    num_buckets =  (max_column_num_bytes + SFRAME_SORT_BUFFER_SIZE - 1) / SFRAME_SORT_BUFFER_SIZE;
    num_buckets = std::max<size_t>(1, num_buckets);

    logstream(LOG_INFO) << "Generating " << num_buckets << " buckets" << std::endl;

    size_t max_column_bytes_per_value = *std::max_element(column_bytes_per_value.begin(),
                                                          column_bytes_per_value.end());
    /*
     * There is a theoretical maximum number of rows we can sort, given
     * max_column_bytes_per_value. We can contain a maximum of 
     * SFRAME_SORT_BUFFER_SIZE / max_column_bytes_per_value values per segment, and
     * we can only construct SFRAME_SORT_MAX_SEGMENTS number of segments.
     */
    size_t max_sort_rows = 
        ((SFRAME_SORT_BUFFER_SIZE * SFRAME_SORT_MAX_SEGMENTS) / max_column_bytes_per_value);
    logstream(LOG_INFO) << "Maximum sort rows: " << max_sort_rows << std::endl;
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
          << std::endl;
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
  std::vector<size_t> bucket_start(num_buckets, 0);
  // due rows_per_bucket being an integer, a degree of imbalance 
  // (up to num_buckets) is expected. That's fine.
  size_t rows_per_bucket = size_t(num_rows) / num_buckets;
  logstream(LOG_INFO) << "Rows per bucket: " << rows_per_bucket << std::endl;
  for (size_t i = 0; i < num_buckets; ++i) {
    bucket_start[i] = i * rows_per_bucket;
  }

  ti.start();
  logstream(LOG_INFO) << "Beginning scatter " << std::endl;
  // Scatter
  // -------
  //  - For each (c,r,v) in data:
  //    Write (c,v) to bucket `Floor(K \ forward_map(r) / N)`
  sframe scatter_sframe = ec_scatter_partitions(values_sframe, 
                                                rows_per_bucket,
                                                num_buckets,
                                                indirect_column,
                                                forward_map);
  logstream(LOG_INFO) << "Scatter finished in " << ti.current_time() << std::endl;
  return std::make_shared<sframe>(scatter_sframe);
}

} // namespace query_eval
} // namespace graphlab
