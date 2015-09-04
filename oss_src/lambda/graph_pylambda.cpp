/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <lambda/graph_pylambda.hpp>
#include <lambda/pyflexible_type.hpp>
#include <lambda/python_api.hpp>
#include <lambda/python_thread_guard.hpp>
#include <lambda/lambda_utils.hpp>
#include <Python.h>
#include <boost/python.hpp>

namespace graphlab {
namespace lambda {

namespace python = boost::python;

typedef python::dict py_vertex_object;
typedef python::dict py_edge_object;

/**************************************************************************/
/*                                                                        */
/*                          pysgraph_synchronize                          */
/*                                                                        */
/**************************************************************************/
void pysgraph_synchronize::init(size_t num_partitions,
                                const std::vector<std::string>& vertex_keys) {
  // clear everything
  m_vertex_partitions.clear();
  m_is_partition_loaded.clear();

  // initialize members
  m_num_partitions = num_partitions;
  m_vertex_partitions.resize(m_num_partitions);
  m_is_partition_loaded.resize(m_num_partitions, false);
  m_vertex_keys = vertex_keys;
}

void pysgraph_synchronize::load_vertex_partition(size_t partition_id, std::vector<sgraph_vertex_data>& vertices) {
  DASSERT_LT(partition_id, m_num_partitions);
  DASSERT_FALSE(m_is_partition_loaded[partition_id]);
  m_vertex_partitions[partition_id] = std::move(vertices);
  m_is_partition_loaded[partition_id] = true;
  DASSERT_TRUE(is_loaded(partition_id));
}

void pysgraph_synchronize::update_vertex_partition(vertex_partition_exchange& vpartition_exchange) {
  DASSERT_TRUE(m_is_partition_loaded[vpartition_exchange.partition_id]);

  auto& vertex_partition = m_vertex_partitions[vpartition_exchange.partition_id];
  auto& fields_ids = vpartition_exchange.field_ids;
  for (auto& vid_data_pair : vpartition_exchange.vertices) {
    size_t id = vid_data_pair.first;
    sgraph_vertex_data& vdata = vid_data_pair.second;
    for (size_t i = 0; i < fields_ids.size(); ++i)
      vertex_partition[id][fields_ids[i]] = vdata[i];
  }
}

vertex_partition_exchange pysgraph_synchronize::get_vertex_partition_exchange(size_t partition_id, const std::unordered_set<size_t>& vertex_ids, const std::vector<size_t>& field_ids) {
  DASSERT_TRUE(m_is_partition_loaded[partition_id]);
  vertex_partition_exchange ret;
  ret.partition_id = partition_id;
  ret.field_ids = field_ids;

  auto& vertex_partition = m_vertex_partitions[partition_id];
  for (size_t vid:  vertex_ids) {
    auto& vdata = vertex_partition[vid];
    sgraph_vertex_data vdata_subset;
    for (auto fid: field_ids) {
      vdata_subset.push_back(vdata[fid]);
    }
    ret.vertices.push_back({vid, std::move(vdata_subset)});
  }
  return ret;
}


/**************************************************************************/
/*                                                                        */
/*                             graph_pylambda                             */
/*                                                                        */
/**************************************************************************/
graph_pylambda_evaluator::graph_pylambda_evaluator() {
  python_thread_guard guard;
  m_current_lambda = new python::object;
}

graph_pylambda_evaluator::~graph_pylambda_evaluator() {
  python_thread_guard guard;
  delete m_current_lambda;
}

void graph_pylambda_evaluator::init(const std::string& lambda,
                                    size_t num_partitions,
                                    const std::vector<std::string>& vertex_fields,
                                    const std::vector<std::string>& edge_fields,
                                    size_t src_column_id,
                                    size_t dst_column_id) {
  clear();

  // initialize members
  make_lambda(lambda);
  m_vertex_keys = vertex_fields;
  m_edge_keys = edge_fields;
  m_srcid_column = src_column_id;
  m_dstid_column = dst_column_id;
  m_graph_sync.init(num_partitions, vertex_fields);
}

void graph_pylambda_evaluator::clear() {
  m_vertex_keys.clear();
  m_edge_keys.clear();
  m_graph_sync.clear();
  m_srcid_column = -1;
  m_dstid_column = -1;
  *m_current_lambda = python::object();
}

std::vector<sgraph_edge_data> graph_pylambda_evaluator::eval_triple_apply(
    const std::vector<sgraph_edge_data>& all_edge_data,
    size_t src_partition, size_t dst_partition,
    const std::vector<size_t>& mutated_edge_field_ids) {

  logstream(LOG_INFO) << "graph_lambda_worker eval triple apply " << src_partition 
                      << ", " << dst_partition << std::endl;
  python_thread_guard guard;
  DASSERT_TRUE(is_loaded(src_partition));
  DASSERT_TRUE(is_loaded(dst_partition));

  py_edge_object edge_object;
  py_vertex_object source_object;
  py_vertex_object target_object;

  auto& source_partition = m_graph_sync.get_partition(src_partition);
  auto& target_partition = m_graph_sync.get_partition(dst_partition);

  std::vector<std::string> mutated_edge_keys;
  for (size_t fid: mutated_edge_field_ids) {
    mutated_edge_keys.push_back(m_edge_keys[fid]);
  }

  std::vector<sgraph_edge_data> ret(all_edge_data.size());
  try {
    size_t cnt = 0;
    for (const auto& edata: all_edge_data) {
      PyDict_UpdateFromFlex(edge_object, m_edge_keys, edata);
      size_t srcid = edata[m_srcid_column];
      size_t dstid = edata[m_dstid_column];

      auto& source_vertex = source_partition[srcid];
      auto& target_vertex = target_partition[dstid];

      PyDict_UpdateFromFlex(source_object, m_vertex_keys, source_vertex);
      PyDict_UpdateFromFlex(target_object, m_vertex_keys, target_vertex);

      python::object lambda_ret = (*m_current_lambda)(source_object, edge_object, target_object);
      if (lambda_ret.is_none() || !PyTuple_Check(lambda_ret.ptr()) || python::len(lambda_ret) != 3) {
        throw(std::string("Lambda must return a tuple of the form (source_data, edge_data, target_data)."));
      }

      for (size_t i = 0; i < m_vertex_keys.size(); ++i) {
        source_vertex[i] = PyObject_AsFlex(lambda_ret[0][m_vertex_keys[i]]);
        target_vertex[i] = PyObject_AsFlex(lambda_ret[2][m_vertex_keys[i]]);
      }

      if (!mutated_edge_field_ids.empty()) {
        edge_object.update(lambda_ret[1]);
        for (auto& key: mutated_edge_keys)
          ret[cnt].push_back(PyObject_AsFlex(edge_object[key]));
      }
      ++cnt;
    }
  } catch (python::error_already_set const& e) {
    std::string error_string = parse_python_error();
    throw(error_string);
  } catch (std::exception& e) {
    throw(std::string(e.what()));
  } catch (const char* e) {
    throw(e);
  } catch (std::string& e) {
    throw(e);
  } catch (...) {
    throw("Unknown exception from python lambda evaluation.");
  }
  return ret;
}

void graph_pylambda_evaluator::make_lambda(const std::string& pylambda_str) {
  python_thread_guard guard;
  try {
    python::object pickle = python::import("pickle");
    PyObject* lambda_bytes = PyByteArray_FromStringAndSize(pylambda_str.c_str(), pylambda_str.size());
    *m_current_lambda = python::object((pickle.attr("loads")(python::object(python::handle<>(lambda_bytes)))));
  } catch (python::error_already_set const& e) {
    std::string error_string = parse_python_error();
    throw(error_string);
  } catch (std::exception& e) {
    throw(std::string(e.what()));
  } catch (...) {
    throw("Unknown exception from python lambda evaluation.");
  }
}

}
}
