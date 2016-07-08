/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_GRAPH_MAP_REDUCE_EDGES_HPP
#define GRAPHLAB_GRAPH_MAP_REDUCE_EDGES_HPP
#include <graph/vertex_set.hpp>
#include <parallel/lambda_omp.hpp>
#include <graphlab/util/generics/conditional_addition_wrapper.hpp>
namespace graphlab {

   /**
    * \brief Performs a map-reduce operation on each edge in the
    * graph returning the result.
    *
    * Given a map function, map_reduce_edges() call the map function on all
    * edges in the graph. The return values are then summed together and the
    * final result returned. The map function should only read data
    * and should not make any modifications. map_reduce_edges() must be
    * called on all machines simultaneously.
    *
    * ### Basic Usage
    * For instance, if the graph has float vertex data, and float edge data:
    * \code
    *   typedef graphlab::distributed_graph<float, float> graph_type;
    * \endcode
    *
    * To compute an absolute sum over all the edge data, we would write
    * a function which reads in each a edge, and returns the absolute
    * value of the data on the edge.
    * \code
    * float absolute_edge_datac(const graph_type::edge_type& edge) {
    *   return std::fabs(edge.data());
    * }
    * \endcode
    * After which calling:
    * \code
    * float sum = graph.map_reduce_edges<float>(absolute_edge_data);
    * \endcode
    * will call the <code>absolute_edge_data()</code> function
    * on each edge in the graph. <code>absolute_edge_data()</code>
    * reads the value of the edge and returns the absolute result.
    * This return values are then summed together and returned.
    * All machines see the same result.
    *
    * The template argument <code><float></code> is needed to inform
    * the compiler regarding the return type of the mapfunction.
    *
    * The two optional arguments vset and edir can be used to restrict the
    * set of edges which are map-reduced over.
    *
    * ### Relations
    * This function similar to
    * graphlab::distributed_graph::map_reduce_edges()
    * with the difference that this does not take a context
    * and thus cannot influence engine signalling.
    * Finally transform_edges() can be used to perform a similar
    * but may also make modifications to graph data.
    *
    * \tparam ReductionType The output of the map function. Must have
    *                    operator+= defined, and must be \ref sec_serializable.
    * \tparam EdgeMapperType The type of the map function.
    *                          Not generally needed.
    *                          Can be inferred by the compiler.
    * \param mapfunction The map function to use. Must take
    *                   a \ref edge_type, or a reference to a
    *                   \ref edge_type as its only argument.
    *                   Returns a ReductionType which must be summable
    *                   and \ref sec_serializable .
    * \param vset A set of vertices. Combines with
    *             edir to identify the set of edges. For instance, if
    *             edir == IN_EDGES, map_reduce_edges will map over all in edges
    *             of the vertices in vset. Optional. Defaults to complete_set().
    * \param edir An edge direction. Combines with vset to identify the set
    *             of edges to map over. For instance, if
    *             edir == IN_EDGES, map_reduce_edges will map over all in edges
    *             of the vertices in vset. Optional. Defaults to IN_EDGES.
    */
    template <typename GraphType, typename ReductionType, typename MapFunctionType>
    ReductionType map_reduce_edges(GraphType& g,
                                   MapFunctionType mapfunction,
                                   const vertex_set& vset = GraphType::complete_set(),
                                   edge_dir_type edir = IN_EDGES) {
      BOOST_CONCEPT_ASSERT((graphlab::Serializable<ReductionType>));
      BOOST_CONCEPT_ASSERT((graphlab::OpPlusEq<ReductionType>));
      typedef typename GraphType::edge_type edge_type;
      typedef typename GraphType::local_edge_type local_edge_type;
      typedef typename GraphType::lvid_type lvid_type;

      if(!g.is_finalized()) {
        logstream(LOG_FATAL)
          << "\n\tAttempting to run graph.map_reduce_vertices(...)"
          << "\n\tbefore calling graph.finalize()."
          << std::endl;
      }

      g.dc().barrier();
      bool global_result_set = false;
      ReductionType global_result = ReductionType();
#ifdef _OPENMP
#pragma omp parallel
#endif
      {
        bool result_set = false;
        ReductionType result = ReductionType();
#ifdef _OPENMP
        #pragma omp for
#endif
        for (int i = 0; i < (int)g.num_local_vertices(); ++i) {
          if (vset.l_contains((lvid_type)i)) {
            if (edir == IN_EDGES || edir == ALL_EDGES) {
              for (const local_edge_type& e: g.l_vertex(i).in_edges()) {
                if (!result_set) {
                  edge_type edge(e);
                  result = mapfunction(edge);
                  result_set = true;
                }
                else if (result_set){
                  edge_type edge(e);
                  const ReductionType tmp = mapfunction(edge);
                  result += tmp;
                }
              }
            }
            if (edir == OUT_EDGES || edir == ALL_EDGES) {
              for (const local_edge_type& e: g.l_vertex(i).out_edges()) {
                if (!result_set) {
                  edge_type edge(e);
                  result = mapfunction(edge);
                  result_set = true;
                }
                else if (result_set){
                  edge_type edge(e);
                  const ReductionType tmp = mapfunction(edge);
                  result += tmp;
                }
              }
            }
          }
        }
#ifdef _OPENMP
        #pragma omp critical
#endif
        {
          if (result_set) {
            if (!global_result_set) {
              global_result = result;
              global_result_set = true;
            }
            else {
              global_result += result;
            }
          }
        }
      }

      conditional_addition_wrapper<ReductionType>
        wrapper(global_result, global_result_set);
      g.dc().all_reduce(wrapper);
      return wrapper.value;
   } // end of map_reduce_edges

} // graphlab
#endif

