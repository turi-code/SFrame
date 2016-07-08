/**
 * Copyright (C) 2016 Turi
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

#ifndef GRAPHLAB_GRAPH_JOIN_HPP
#define GRAPHLAB_GRAPH_JOIN_HPP
#include <utility>
#include <boost/unordered_map.hpp>
#include <graphlab/util/hopscotch_map.hpp>
#include <graph/distributed_graph.hpp>
#include <rpc/dc_dist_object.hpp>
namespace graphlab {


/**
 * \brief Provides the ability to pass information between vertices of two 
 * different graphs
 *
 * \tparam LeftGraph Type of the left graph 
 * \tparam RightGraph Type of the right graph 

 * The graph_vertex_join class allows information to be passed between
 * vertices of two different graphs. 
 * 
 * Given two graphs <code>g1</code> and <code>g2</code>, possibly of different 
 * types:
 * 
 * \code
 * typedef distributed_graph<VData1, EData1> graph_1_type;
 * typedef distributed_graph<VData2, EData2> graph_2_type;
 * graph_1_type g1;
 * graph_2_type g2;
 * \endcode
 *
 * A graph_vertex_join object can be created:
 * \code
 * graph_vertex_type<graph_1_type, graph_2_type> vjoin(dc, g1, g2);
 * \endcode
 *
 * The first argument is the distributed control object. The second argument
 * shall be referred to as the graph on the "left" side of the join, and the 
 * third argument shall be referred to as the graph on the "right" side of the
 * join.
 *
 * The join operates by having each vertex in both graph emit an integer key.
 * Vertices with the same key are then combined into the same group. The 
 * semantics of the key depends on the join operation to be performed.
 * Right now, the only join operation supported is the Left Injective Join
 * and the Right Injective Join (see below).
 *
*
 *
 * ## Left Injective Join
 * For the left injective join, vertices in the same graph must emit distinct
 * unique keys. i.e. Each vertex in <code>g1</code> must emit a key which is 
 * different from all other vertices in <code>g1</code>. Vertices on the right
 * graph are then matched with vertices on the left graph with the same key.
 * The join operation is then allowed to modify vertices on the left graph 
 * given the data on the vertices of the right graph.
 * 
 * To emit the keys:
 * \code
 * vjoin.prepare_injective_join(left_emit_key, right_emit_key);
 * \endcode
 * left_emit_key and right_emit_key are functions (or lambda) with the following 
 * prototype:
 * \code
 * size_t left_emit_key(const graph_1_type::vertex_type& vertex);
 * size_t right_emit_key(const graph_2_type::vertex_type& vertex);
 * \endcode
 * They essentially take as a constant argument, the vertex of their respective
 * graphs, and return an integer key. If the key has value (-1) it does not 
 * participate in the join.
  * After keys are emitted and prepared with prepare_join, to perform a left
 * injective join:
 * \code
 * vjoin.left_injective_join(join_op);
 * \endcode
 * Where join_op is a function with the following prototype:
 * \code
 * void join_op(graph_1_type::vertex_type& left_vertex, 
 *              const graph_2_type::vertex_data_type right_vertex_data);
 * \endcode
 * Note the asymmetry in the arguments: the left vertex is passed as a 
 * vertex_type, while for the right vertex, only the vertex data is accessible.
 * The function may make modifications on the left vertex.
 * 
 * The left_injective_join() function must be called by all machines.
 * As a result, it may be used from within an engine's 
 * \ref graphlab::iengine::add_vertex_aggregator aggregator's finalize
 * function.
 *
 * ### Left Injective Join Example
 * I have two graphs with identical structure. The left graph has data
 * \code
 * struct left_vertex_data {
 *   size_t user_id;
 *   std::string user_name;
 *   std::string email_address;
 *   // ... serializers omitted ...
 * }
 * typedef distributed_graph<left_vertex_data, some_edge_data> left_graph_type;
 * \endcode
 * However, when the left graph was constructed, there was no email address
 * information conveniently available, and thus was left blank.
 *
 * And the right graph has vertex data: 
 * \code
 * struct right_vertex_data {
 *   size_t user_id;
 *   std::string email_address;
 *   // ... serializers omitted ...
 * }
 * typedef distributed_graph<right_vertex_data, some_edge_data> right_graph_type;
 * \endcode
 * which was loaded from some other source, and contains all the email address
 * information.
 *
 * I create emit functions for both graphs :
 * \code
 * size_t emit_user_id_field_left(const left_graph_type::vertex_type& vtype) {
 *   return vtype.data().user_id;
 * }
 * size_t emit_user_id_field_left(const right_graph_type::vertex_type& vtype) {
 *   return vtype.data().user_id;
 * }
 * \endcode
 *
 * Create a join object and prepare the join:
 * \code
 * graph_vertex_join<left_graph_type, right_graph_type> vjoin(dc, 
 *                                                            left_graph, 
 *                                                            right_graph);
 * vjoin.prepare_injective_join(emit_user_id_field_left, 
 *                              emit_user_id_field_right);
 * \endcode
 *
 * To copy the email address field from the right graph to the left graph:
 * \code
 * void join_email_address(left_graph_type::vertex_type& left_vertex,
 *                         const right_vertex_data& rvtx) {
 *   left_vertex.data().email_address = rvtx.email_address; 
 * }
 * 
 * vjoin.left_injective_join(join_email_address);
 * \endcode
 * 
 * ## Right Injective Join
 * The right injective join is similar to the left injective join, but
 * with types reversed.
 */
template <typename LeftGraph, typename RightGraph> 
class graph_vertex_join {
  public:
    /// Type of the left graph
    typedef LeftGraph left_graph_type;
    /// Type of the right graph
    typedef RightGraph right_graph_type;
    /// Vertex Type of the left graph
    typedef typename right_graph_type::vertex_type left_vertex_type;
    /// Vertex Type of the right graph
    typedef typename left_graph_type::vertex_type right_vertex_type;
    /// Local Vertex Type of the left graph
    typedef typename right_graph_type::local_vertex_type left_local_vertex_type;
    /// Local Vertex Type of the right graph
    typedef typename left_graph_type::local_vertex_type right_local_vertex_type;
     /// Vertex Data Type of the left graph
    typedef typename right_graph_type::vertex_data_type left_data_type;
    /// Vertex Data Type of the right graph
    typedef typename left_graph_type::vertex_data_type right_data_type;

    dc_dist_object<graph_vertex_join<LeftGraph, RightGraph> > rmi;

  private:
    /// Reference to the left graph
    left_graph_type& left_graph;
    /// Reference to the right graph
    right_graph_type& right_graph;
    
    struct injective_join_index {
      std::vector<size_t> vtx_to_key;
      hopscotch_map<size_t, vertex_id_type> key_to_vtx;
      // we use -1 here to indicate that the vertex is not participating
      std::vector<procid_t> opposing_join_proc;
    };

    injective_join_index left_inj_index, right_inj_index;

  public:
    graph_vertex_join(distributed_control& dc,
                      left_graph_type& left,
                      right_graph_type& right): 
        rmi(dc, this), left_graph(left), right_graph(right) { }


    /**
      * \brief Initializes the join by associating each vertex with a key
      *
      * \tparam LeftEmitKey Type of the left_emit_key parameter. It should
      *   not be necessary to specify this. C++ type inference should be able
      *   to infer this automatically.
      * \tparam RightEmitKey Type of the right_emit_key parameter. It should
      *   not be necessary to specify this. C++ type inference should be able
      *   to infer this automatically.
      *
      * \param left_emit_key A function which takes a vertex_type from the
      *  left graph and emits an integral key value. 
      *  Can be a lambda, of the prototype:
      *   size_t left_emit_key(const LeftGraph::vertex_type& vertex);
      * \param right_emit_key A function which takes a vertex_type from the
      *  right graph and emits an integral key value. 
      *  Can be a lambda, of the prototype:
      *   size_t right_emit_key(const RightGraph::vertex_type& vertex);
      *
      * The semantics of the key depend on the actual join operation performed.
      * This function must be called by all machines.
      *
      * left_emit_key and right_emit_key are functions (or lambda) with the 
      * following prototype:
      * \code
      * size_t left_emit_key(const graph_1_type::vertex_type& vertex);
      * size_t right_emit_key(const graph_2_type::vertex_type& vertex);
      * \endcode
      * They essentially take as a constant argument, the vertex of their 
      * respective graphs, and return an integer key. If a vertex emits the key
      * (size_t)(-1) it does not participate in the join.
      *
      * prepare_injective_join() only needs to be called once. After which an 
      * arbitrary number of left_injective_join() and right_injective_join() 
      * calls may be made.
      *
      * If after a join, a new join is to be performed on the same graph using
      * new data, or new emit functions, prepare_injective_join() can be called
      * again to recompute the join. 
      */
    template <typename LeftEmitKey, typename RightEmitKey>
    void prepare_injective_join(LeftEmitKey left_emit_key, 
                                RightEmitKey right_emit_key) {
      typedef std::pair<size_t, vertex_id_type> key_vertex_pair;
      // Basically, what we are trying to do is to figure out, for each vertex
      // on one side of the graph, which vertices for the other graph
      // (and on on which machines) emitted the same key.
      //
      // The target datastructure is:
      // vtx_to_key[vtx]: The key for each vertex
      // opposing_join_proc[vtx]: Machines which hold a vertex on the opposing
      //                          graph which emitted the same key
      // key_to_vtx[key] Mapping of keys to vertices. 
      
      // resize the left index
      // resize the right index

      reset_and_fill_injective_index(left_inj_index, 
                                     left_graph, 
                                     left_emit_key, "left graph");    

      reset_and_fill_injective_index(right_inj_index, 
                                     right_graph, 
                                     right_emit_key, "right graph");    
      rmi.barrier(); 
      // now, we need cross join across all machines to figure out the 
      // opposing join proc
      // we need to do this twice. Once for left, and once for right. 
      compute_injective_join();
    }

    /**
     * \brief Performs an injective join from the right graph to the left graph.
     * 
     * \tparam JoinOp The type of the joinop function. It should
     *   not be necessary to specify this. C++ type inference should be able
      *   to infer this automatically.
     *
     * \param join_op The joining function. May be a function pointer or a 
     * lambda matching the prototype
     * void join_op(LeftGraph::vertex_type& left_vertex, 
     *              const RightGraph::vertex_data_type right_vertex_data);
     * 
     * prepare_injective_join() must be called before hand.
     * All machines must call this function. join_op will be called on each
     * left vertex with the data on a right vertex which emitted the same key
     * in prepare_injective_join(). The join_op function is allowed to modify
     * the vertex data on the left graph.
     */
    template <typename JoinOp>
    void left_injective_join(JoinOp join_op) {
      injective_join(left_inj_index, left_graph,
                     right_inj_index, right_graph, 
                     join_op);
    }


    /**
     * \brief Performs an injective join from the left graph to the right graph.
     * 
     * \tparam JoinOp The type of the joinop function. It should
     *   not be necessary to specify this. C++ type inference should be able
      *   to infer this automatically.
     *
     * \param join_op The joining function. May be a function pointer or a 
     * lambda matching the prototype
     * void join_op(RightGraph::vertex_type& right_vertex, 
     *              const LeftGraph::vertex_data_type left_vertex_data);
     * 
     * prepare_injective_join() must be called before hand.
     * All machines must call this function. join_op will be called on each
     * rght vertex with the data on a left vertex which emitted the same key
     * in prepare_injective_join(). The join_op function is allowed to modify
     * the vertex data on the right graph.
     */
    template <typename JoinOp>
    void right_injective_join(JoinOp join_op) {
      injective_join(right_inj_index, right_graph,
                     left_inj_index, left_graph, 
                     join_op);
    }

  private:
    template <typename Graph, typename EmitKey>
    void reset_and_fill_injective_index(injective_join_index& idx,
                                        Graph& graph,
                                        EmitKey& emit_key,
                                        const char* message) {
      // clear the data
      idx.vtx_to_key.resize(graph.num_local_vertices());
      idx.key_to_vtx.clear(); 
      idx.opposing_join_proc.resize(graph.num_local_vertices(), (procid_t)(-1));
      // loop through vertices, get the key and fill vtx_to_key and key_to_vtx
      for(lvid_type v = 0; v < graph.num_local_vertices(); ++v) {
        typename Graph::local_vertex_type lv = graph.l_vertex(v);
        if (lv.owned()) {
          typename Graph::vertex_type vtx(lv);
          size_t key = emit_key(vtx);
          idx.vtx_to_key[v] = key;
          if (key != (size_t)(-1)) {
            if (idx.key_to_vtx.count(key) > 0) {
              logstream(LOG_ERROR) << "Duplicate key in " << message << std::endl;
              logstream(LOG_ERROR) << "Duplicate keys not permitted" << std::endl;
              throw "Duplicate Key in Join";
            }
            idx.key_to_vtx.insert(std::make_pair(key, v));
          }
        }
      }
    }

    void compute_injective_join() {
      std::vector<std::vector<size_t> > left_keys = 
          get_procs_with_keys(left_inj_index.vtx_to_key, left_graph);
      std::vector<std::vector<size_t> > right_keys = 
          get_procs_with_keys(right_inj_index.vtx_to_key, right_graph);
      // now. for each key on the right, I need to figure out which proc it
      // belongs in. and vice versa. This is actually kind of annoying.
      // but since it is one-to-one, I only need to make a hash map of one side.
      hopscotch_map<size_t, procid_t> left_key_to_procs;

      // construct a hash table of keys to procs
      // clear frequently to use less memory
      for (size_t p = 0; p < left_keys.size(); ++p) {
        for (size_t i = 0; i < left_keys[p].size(); ++i) {
          ASSERT_MSG(left_key_to_procs.count(left_keys[p][i]) == 0,
                     "Duplicate keys not permitted for left graph keys in injective join");
          left_key_to_procs.insert(std::make_pair(left_keys[p][i], p));
        }
        std::vector<size_t>().swap(left_keys[p]);
      }
      left_keys.clear();
     
      std::vector<
          std::vector<
              std::pair<size_t, procid_t> > > left_match(rmi.numprocs());
      std::vector<
          std::vector<
              std::pair<size_t, procid_t> > > right_match(rmi.numprocs());

      // now for each key on the right, find the matching key on the left
      for (size_t p = 0; p < right_keys.size(); ++p) {
        for (size_t i = 0; i < right_keys[p].size(); ++i) {
          size_t key = right_keys[p][i];
          hopscotch_map<size_t, procid_t>::iterator iter =
              left_key_to_procs.find(key);
          if (iter != left_key_to_procs.end()) {
            ASSERT_MSG(iter->second != (procid_t)(-1),
                       "Duplicate keys not permitted for right graph keys in injective join");
            // we have a match
            procid_t left_proc = iter->second;
            procid_t right_proc = p;
            // now. left has to be told about right and right
            // has to be told about left
            left_match[left_proc].push_back(std::make_pair(key, right_proc));
            right_match[right_proc].push_back(std::make_pair(key, left_proc));
            // set the map entry to -1 
            // so we know if it is ever reused
            iter->second = (procid_t)(-1); 
          }
        }
        std::vector<size_t>().swap(right_keys[p]);
      }
      right_keys.clear();

      rmi.all_to_all(left_match);
      rmi.all_to_all(right_match);
      // fill in the index
      // go through the left match and set up the opposing index to based
      // on the match result
#ifdef _OPENMP
#pragma omp parallel for
#endif
      for (size_t p = 0;p < left_match.size(); ++p) {
        for (size_t i = 0;i < left_match[p].size(); ++i) {
          // search for the key in the left index
          hopscotch_map<size_t, vertex_id_type>::const_iterator iter = 
              left_inj_index.key_to_vtx.find(left_match[p][i].first);
          ASSERT_TRUE(iter != left_inj_index.key_to_vtx.end());
          // fill in the match
          left_inj_index.opposing_join_proc[iter->second] = left_match[p][i].second;
        }
      }
      left_match.clear();
      // repeat for the right match
#ifdef _OPENMP
#pragma omp parallel for
#endif
      for (size_t p = 0;p < right_match.size(); ++p) {
        for (size_t i = 0;i < right_match[p].size(); ++i) {
          // search for the key in the right index
          hopscotch_map<size_t, vertex_id_type>::const_iterator iter = 
              right_inj_index.key_to_vtx.find(right_match[p][i].first);
          ASSERT_TRUE(iter != right_inj_index.key_to_vtx.end());
          // fill in the match
          right_inj_index.opposing_join_proc[iter->second] = right_match[p][i].second;
        }
      }
      right_match.clear();
      // ok done.
    }

    // each key is assigned to a controlling machine, who receives
    // the partial list of keys every other machine owns.
    template <typename Graph>
    std::vector<std::vector<size_t> > 
        get_procs_with_keys(const std::vector<size_t>& local_key_list, Graph& g) {
      // this machine will get all keys from each processor where
      // key = procid mod numprocs
      std::vector<std::vector<size_t> > procs_with_keys(rmi.numprocs());
      for (size_t i = 0; i < local_key_list.size(); ++i) {
        if (g.l_vertex(i).owned() && local_key_list[i] != (size_t)(-1)) {
          procid_t target_procid = local_key_list[i] % rmi.numprocs();
          procs_with_keys[target_procid].push_back(local_key_list[i]);
        }
      }
      rmi.all_to_all(procs_with_keys);
      return procs_with_keys;
    }

    template <typename TargetGraph, typename SourceGraph, typename JoinOp>
    void injective_join(injective_join_index& target,
                        TargetGraph& target_graph,
                        injective_join_index& source,
                        SourceGraph& source_graph,
                        JoinOp joinop) {
      // build up the exchange structure.
      // move right vertex data to left
      std::vector<
          std::vector<
              std::pair<size_t, typename SourceGraph::vertex_data_type> > > 
            source_data(rmi.numprocs());

      for (size_t i = 0; i < source.opposing_join_proc.size(); ++i) {
        if (source_graph.l_vertex(i).owned()) {
          procid_t target_proc = source.opposing_join_proc[i];
          if (target_proc >= 0 && target_proc < rmi.numprocs()) {
            source_data[target_proc].push_back(
                std::make_pair(source.vtx_to_key[i],
                               source_graph.l_vertex(i).data()));
          }
        }
      }
      // exchange
      rmi.all_to_all(source_data);
      // ok. now join against left
#ifdef _OPENMP
#pragma omp parallel for
#endif
      for (size_t p = 0;p < source_data.size(); ++p) {
        for (size_t i = 0;i < source_data[p].size(); ++i) {
          // find the target vertex with the matching key
          hopscotch_map<size_t, vertex_id_type>::const_iterator iter = 
              target.key_to_vtx.find(source_data[p][i].first);
          ASSERT_TRUE(iter != target.key_to_vtx.end());
          // found it!
          typename TargetGraph::local_vertex_type 
              lvtx = target_graph.l_vertex(iter->second);
          typename TargetGraph::vertex_type vtx(lvtx);
          joinop(vtx, source_data[p][i].second);
        }
      }
      target_graph.synchronize();
    }
};

} // namespace graphlab

#endif
