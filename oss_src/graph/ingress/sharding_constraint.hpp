/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
/**  
 * Copyright (c) 2009 Carnegie Mellon University. 
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS
 *  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied.  See the License for the specific language
 *  governing permissions and limitations under the License.
 *
 * For more about this software visit:
 *
 *      http://www.graphlab.ml.cmu.edu
 *
 */

#ifndef GRAPHLAB_DISTRIBUTED_SHARDING_CONSTRAINT_HPP
#define GRAPHLAB_DISTRIBUTED_SHARDING_CONSTRAINT_HPP

#include <graph/graph_basic_types.hpp>
#include <graphlab/util/generate_pds.hpp>
#include <algorithm>
#include <vector>


/**
 * This class defines the dependencies among the shards when using
 * a constrained partitioning algorithm.
 *
 * In constrained partitioning, vertices are assgined to a master shard
 * using hash function on the vids.  Each shard S masters a partition of 
 * vertices: V_s. 
 *
 * Let Ai be the set of shards that Shard i depends on. Then the partitioning
 * algorithm can only put edges with either ends in V_si into Ai. For example,
 * Shard i is the master of vertex u, and Shard j is the master of vertex v,
 * then edge u->v must be placed into Ai \intersect Aj.
 *
 * This class currently has two implementations of the shard constraints. One
 * construction is based on a grid, and the other is based on perfect difference set.
 * Both algorithms guarentees that Ai \intersect Aj is non-empty.
 * 
 * \note: grid methods requires the number of shards to be a perfect square number. pds
 * requires the number of shards to be p^2 + p + 1 where p is a prime number.
 * 
 */
namespace graphlab {
  class sharding_constraint {
    size_t nshards;
    std::vector<std::vector<procid_t> > constraint_graph;

    std::vector<std::vector<std::vector<procid_t> > > joint_nbr_cache;
   public:
    /// Test if the provided num_shards can be used for grid construction: 
    //    n == nrow*ncol  && (abs(nrow-ncol) <= 2)
    static bool is_grid_compatible(size_t num_shards, int& nrow, int& ncol) {
      double approx_sqrt = sqrt(num_shards);
      nrow = floor(approx_sqrt);
      for (ncol = nrow; ncol <= nrow + 2; ++ncol) {
        if (ncol * nrow == (int)num_shards) {
          return true;
        }
      }
      return false;
    }

    static bool is_pds_compatible(size_t num_shards, int& p) {
      p = floor(sqrt(num_shards-1));
      return (p>0 && ((p*p+p+1) == (int)num_shards));
    }

   public:
    sharding_constraint(size_t num_shards, std::string method) {
      nshards = num_shards;
      // ignore the method input for now, only construct grid graph. 
      // assuming nshards is perfect square
      if (method == "grid") {
        make_grid_constraint();
      } else if (method == "pds") {
        make_pds_constraint();
      } else {
        logstream(LOG_FATAL) << "Unknown sharding constraint method: " << method << std::endl;
      }

      joint_nbr_cache.resize(num_shards);
      for (size_t i = 0; i < num_shards; ++i) {
        joint_nbr_cache[i].resize(num_shards);
        for (size_t j = 0; j < num_shards; ++j) {
          compute_neighbors(i, j, joint_nbr_cache[i][j]);
          ASSERT_GT(joint_nbr_cache[i][j].size(), 0);
        }
      }
    }

    bool get_neighbors (procid_t shard, std::vector<procid_t>& neighbors) {
      ASSERT_LT(shard, nshards);
      neighbors.clear();
      std::vector<procid_t>& ls = constraint_graph[shard];
      for (size_t i = 0; i < ls.size(); ++i)
        neighbors.push_back(ls[i]);
      return true;
    }

    
    const std::vector<procid_t>& get_joint_neighbors (procid_t shardi, procid_t shardj) {
      return joint_nbr_cache[shardi][shardj];
    }

   private:
    void make_grid_constraint() {
      int ncols, nrows;
      if (!is_grid_compatible(nshards, nrows, ncols)) {
        logstream(LOG_FATAL) << "Num shards: " << nshards << " cannot be used for grid ingress." << std::endl;
      };

      for (size_t i = 0; i < nshards; i++) {
        std::vector<procid_t> adjlist;
        // add self
        adjlist.push_back(i);

        // add the row of i
        size_t rowbegin = (i/ncols) * ncols;
        for (size_t j = rowbegin; j < rowbegin + ncols; ++j)
          if (i != j) adjlist.push_back(j); 

        // add the col of i
        for (size_t j = i % ncols; j < nshards; j+=ncols)
          if (i != j) adjlist.push_back(j); 

        std::sort(adjlist.begin(), adjlist.end());
        constraint_graph.push_back(adjlist);
      }
    }

    void make_pds_constraint() {
      int p = 0;
      if (!is_pds_compatible(nshards, p)) {
        logstream(LOG_FATAL) << "Num shards: " << nshards << " cannot be used for pdsingress." << std::endl;
      };
      pds pds_generator;
      std::vector<size_t> results;
      if (p == 1) {
        results.push_back(0);
        results.push_back(2);
      } else {
        results = pds_generator.get_pds(p);
      }
      for (size_t i = 0; i < nshards; i++) {
        std::vector<procid_t> adjlist;
        for (size_t j = 0; j < results.size(); j++) {
          adjlist.push_back( (results[j] + i) % nshards);
        }
        std::sort(adjlist.begin(), adjlist.end());
        constraint_graph.push_back(adjlist);
      }
    }


    bool compute_neighbors(procid_t shardi, procid_t shardj, std::vector<procid_t>& neighbors) {
      ASSERT_EQ(neighbors.size(), 0);
      ASSERT_LT(shardi, nshards);
      ASSERT_LT(shardj, nshards);
      // if (shardi == shardj) {
      //   neighbors.push_back(shardi);
      //   return true;
      // }

      std::vector<procid_t>& ls1 = constraint_graph[shardi];
      std::vector<procid_t>& ls2 = constraint_graph[shardj];
      neighbors.clear();
      size_t i = 0;
      size_t j = 0;
      while (i < ls1.size() && j < ls2.size()) {
        if (ls1[i] == ls2[j]) {
          neighbors.push_back(ls1[i]);
          ++i; ++j;
        } else if (ls1[i] < ls2[j]) {
          ++i;
        } else {
          ++j;
        }
      }
      return true;
    }

  }; // end of sharding_constraint
}; // end of namespace graphlab
#endif
