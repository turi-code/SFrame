/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <dot_graph_printer/dot_graph.hpp>
#include <sframe_query_engine/execution/execution_node.hpp>
#include <sframe_query_engine/planning/planner_node.hpp>
#include <sframe_query_engine/execution/subplan_executor.hpp> 
#include <sframe_query_engine/operators/operator_transformations.hpp>
#include <sframe_query_engine/operators/all_operators.hpp>
#include <sframe_query_engine/planning/planner.hpp>
#include <sframe_query_engine/planning/optimization_engine.hpp>
#include <sframe_query_engine/query_engine_lock.hpp>
#include <globals/globals.hpp>
#include <sframe/sframe.hpp>

namespace graphlab { namespace query_eval {

size_t SFRAME_MAX_LAZY_NODE_SIZE = 10000;

REGISTER_GLOBAL(int64_t, SFRAME_MAX_LAZY_NODE_SIZE, true);


/**
 * Directly executes a linear query plan potentially parallelizing it if possible.
 * No fast path optimizations. You should use execute_node.
 */
static sframe execute_node_impl(pnode_ptr input_n, const materialize_options& exec_params) {
  // Either run directly, or split it up into a parallel section
  if(is_parallel_slicable(input_n) && (exec_params.num_segments != 0)) {

    size_t num_segments = exec_params.num_segments;

    std::vector<pnode_ptr> segments(num_segments);

    for(size_t segment_idx = 0; segment_idx < num_segments; ++segment_idx) {
      std::map<pnode_ptr, pnode_ptr> memo;
      segments[segment_idx] = make_segmented_graph(input_n, segment_idx, num_segments, memo);
    }

    return subplan_executor().run_concat(segments, exec_params);
  } else {
    return subplan_executor().run(input_n, exec_params);
  }
}

/**
 * Executes a linear query plan, potentially parallelizing it if possible.
 * Also implements fast paths in the event the input node is a source node.
 */
static sframe execute_node(pnode_ptr input_n, const materialize_options& exec_params) {
  // fast path for SFRAME_SOURCE. If I am not streaming into
  // a callback, I can just call save
  if (exec_params.write_callback == nullptr &&
      input_n->operator_type == planner_node_type::SFRAME_SOURCE_NODE) {
    auto sf = input_n->any_operator_parameters.at("sframe").as<sframe>();
    if (input_n->operator_parameters.at("begin_index") == 0 &&
        input_n->operator_parameters.at("end_index") == sf.num_rows()) {
      if (!exec_params.output_index_file.empty()) {
        if (!exec_params.output_column_names.empty()) {
          ASSERT_EQ(sf.num_columns(), exec_params.output_column_names.size());
        }
        for (size_t i = 0;i < sf.num_columns(); ++i) {
          sf.set_column_name(i, exec_params.output_column_names[i]);
        }
        sf.save(exec_params.output_index_file);
      }
      return sf;
    } else {
    }
    // fast path for SARRAY_SOURCE . If I am not streaming into
    // a callback, I can just call save
  } else if (exec_params.write_callback == nullptr &&
             input_n->operator_type == planner_node_type::SARRAY_SOURCE_NODE) {
    const auto& sa = input_n->any_operator_parameters.at("sarray").as<std::shared_ptr<sarray<flexible_type>>>();
    if (input_n->operator_parameters.at("begin_index") == 0 &&
        input_n->operator_parameters.at("end_index") == sa->size()) {
      sframe sf({sa},{"X1"});
      if (!exec_params.output_index_file.empty()) {
        if (!exec_params.output_column_names.empty()) {
          ASSERT_EQ(1, exec_params.output_column_names.size());
        }
        sf.set_column_name(0, exec_params.output_column_names.at(0));
        sf.save(exec_params.output_index_file);
      }
      return sf;
    }
    // if last node is a generalized union project and some come direct from sources
    // we can take advantage that sarray columns are "moveable" and
    // just materialize the modified columns
  } else if (exec_params.write_callback == nullptr &&
             input_n->operator_type == planner_node_type::GENERALIZED_UNION_PROJECT_NODE) {
    if (input_n->any_operator_parameters.count("direct_source_mapping")) {
      // ok we have a list of direct sources we don't need to rematerialize in the
      // geneneralized union project.
      auto existing_columns = input_n->any_operator_parameters
          .at("direct_source_mapping")
          .as<std::map<size_t, std::shared_ptr<sarray<flexible_type>>>>();

      // if there are no existing columns, there is nothing to optimize
      if (!existing_columns.empty()) {
        auto ncolumns = infer_planner_node_num_output_columns(input_n);

        // the indices the columns to materialize. we will project this out
        // and materialize them
        std::vector<size_t> columns_to_materialize;
        // The final set of sframe columns;
        // we fill in what we know first from existing_columns
        std::vector<std::shared_ptr<sarray<flexible_type>>> resulting_sframe_columns(ncolumns);
        for (size_t i = 0;i < ncolumns; ++i) {
          auto iter = existing_columns.find(i);
          if (iter == existing_columns.end()) {
            // leave a gap in resulting_sframe_columns we will fill these in later
            columns_to_materialize.push_back(i);
          } else {
            resulting_sframe_columns[i] = iter->second;
          }
        }
        if (!columns_to_materialize.empty()) {
          // add a project to the end selecting just this set of columns
          // clear the column names if any. don't need them
          auto new_exec_params = exec_params;
          new_exec_params.output_column_names.clear();
          input_n = op_project::make_planner_node(input_n, columns_to_materialize);
          input_n = optimization_engine::optimize_planner_graph(input_n, new_exec_params);
          logstream(LOG_INFO) << "Materializing only column subset: " << input_n << std::endl;

          sframe new_columns = execute_node_impl(input_n, new_exec_params);
          // rebuild an sframe
          // fill in the gaps in resulting_sframe_columns
          // these are the columns we just materialized
          for (size_t i = 0; i < columns_to_materialize.size(); ++i) {
            resulting_sframe_columns[columns_to_materialize[i]] = new_columns.select_column(i);
          }
        }
        return sframe(resulting_sframe_columns,
                      exec_params.output_column_names);
      }
    }
  }
  return execute_node_impl(input_n, exec_params);
}
////////////////////////////////////////////////////////////////////////////////

sframe planner::materialize(pnode_ptr ptip, 
                            materialize_options exec_params) {
  std::lock_guard<recursive_mutex> GLOBAL_LOCK(global_query_lock);
  if (exec_params.num_segments == 0) {
    exec_params.num_segments = thread::cpu_count();
  }
  auto original_ptip = ptip;
  // Optimize Query Plan
  if (!is_source_node(ptip)) {
    logstream(LOG_INFO) << "Materializing: " << ptip << std::endl;
  }
  if(!exec_params.disable_optimization) {
    ptip = optimization_engine::optimize_planner_graph(ptip, exec_params);
    if (!is_source_node(ptip)) {
      logstream(LOG_INFO) << "Optimized As: " << ptip << std::endl;
    }
  }

  ////////////////////////////////////////////////////////////////////////////////
  // Execute stuff.
  pnode_ptr final_node = ptip;


  if (exec_params.write_callback == nullptr) {
    // no write callback
    // Rewrite the query node to be materialized source node
    auto ret_sf = execute_node(final_node, exec_params);
    (*original_ptip) = (*(op_sframe_source::make_planner_node(ret_sf)));
    return ret_sf;
  } else {
    // there is a callback. push it through to execute parameters.
    return execute_node(final_node, exec_params);
  }
}

void planner::materialize(std::shared_ptr<planner_node> tip, 
                          write_callback_type callback,
                          size_t num_segments) {
  materialize_options args;
  args.num_segments = num_segments;
  args.write_callback = callback;
  materialize(tip, args);
};


  /** If this returns true, it is recommended to go ahead and
   *  materialize the sframe operations on the fly to prevent memory
   *  issues.
   */
bool planner::online_materialization_recommended(std::shared_ptr<planner_node> tip) {
  size_t lazy_node_size = infer_planner_node_num_dependency_nodes(tip);
  return lazy_node_size >= SFRAME_MAX_LAZY_NODE_SIZE;
}

/**  
 * Materialize the output, returning the result as a planner node.
 */
std::shared_ptr<planner_node>  planner::materialize_as_planner_node(
    std::shared_ptr<planner_node> tip, materialize_options exec_params) {

  sframe res = materialize(tip, exec_params);
  return op_sframe_source::make_planner_node(res);
}

}}
