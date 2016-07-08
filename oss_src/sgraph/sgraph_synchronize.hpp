/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_SGRAPH_SGRAPH_SYNCHRONIZE_HPP
#define GRAPHLAB_SGRAPH_SGRAPH_SYNCHRONIZE_HPP

#include<sgraph/sgraph_synchronize_interface.hpp>
#include<logger/assertions.hpp>

namespace graphlab {
namespace sgraph_compute {

class sgraph_synchronize : public sgraph_synchronize_interface {
 public:

  sgraph_synchronize() { }

  sgraph_synchronize(size_t num_partitions) {
    init(num_partitions);
  }

  void init(size_t num_partitions) {
    m_vertex_partitions.clear();
    m_is_partition_loaded.clear();

    m_num_partitions = num_partitions;
    m_vertex_partitions.resize(num_partitions);
    m_is_partition_loaded.resize(num_partitions, false);
  }

  inline void load_vertex_partition(size_t partition_id, std::vector<sgraph_vertex_data>& vertices) {
    DASSERT_LT(partition_id, m_num_partitions);
    DASSERT_FALSE(m_is_partition_loaded[partition_id]);
    m_vertex_partitions[partition_id] = &vertices;
    m_is_partition_loaded[partition_id] = true;
  }

  inline void update_vertex_partition(vertex_partition_exchange& vpartition_exchange) {
    DASSERT_TRUE(m_is_partition_loaded[vpartition_exchange.partition_id]);

    auto& vertex_partition = *(m_vertex_partitions[vpartition_exchange.partition_id]);
    auto& update_field_index = vpartition_exchange.field_ids; 

    for (auto& vid_data_pair : vpartition_exchange.vertices) {
      size_t id = vid_data_pair.first;
      sgraph_vertex_data& vdata = vid_data_pair.second;
      for (size_t i = 0; i < update_field_index.size(); ++i) {
        size_t fid = vpartition_exchange.field_ids[i];
        vertex_partition[id][fid] = vdata[i];
      }
    }
  }

  inline vertex_partition_exchange get_vertex_partition_exchange(size_t partition_id, const std::unordered_set<size_t>& vertex_ids, const std::vector<size_t>& field_ids) {
    DASSERT_TRUE(m_is_partition_loaded[partition_id]);
    vertex_partition_exchange ret;
    ret.partition_id = partition_id;
    ret.field_ids = field_ids;
    auto& vertex_partition = *(m_vertex_partitions[partition_id]);
    for (size_t vid:  vertex_ids) {
      auto& vdata = vertex_partition[vid];
      sgraph_vertex_data vdata_subset;
      for (auto fid: field_ids)  vdata_subset.push_back(vdata[fid]);
      ret.vertices.push_back({vid, std::move(vdata_subset)});
    }
    return ret;
  }

 private:
  std::vector<std::vector<sgraph_vertex_data>*> m_vertex_partitions;
  std::vector<bool> m_is_partition_loaded;
  size_t m_num_partitions;
};

} // end sgraph_compute 
} // end graphlab

#endif
