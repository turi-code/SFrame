/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_GRAPH_MAP_REDUCE_VERTICES_HPP
#define GRAPHLAB_GRAPH_MAP_REDUCE_VERTICES_HPP
#include <graph/vertex_set.hpp>
#include <parallel/lambda_omp.hpp>
#include <graphlab/util/generics/conditional_addition_wrapper.hpp>
namespace graphlab {
   /**
    * \brief Performs a map-reduce operation on each vertex in the
    * graph returning the result.
    *
    * Given a map function, map_reduce_vertices() call the map function on all
    * vertices in the graph. The return values are then summed together and the
    * final result returned. The map function should only read the vertex data
    * and should not make any modifications. map_reduce_vertices() must be
    * called on all machines simultaneously.
    *
    * ### Basic Usage
    * For instance, if the graph has float vertex data, and float edge data:
    * \code
    *   typedef graphlab::distributed_graph<float, float> graph_type;
    * \endcode
    *
    * To compute an absolute sum over all the vertex data, we would write
    * a function which reads in each a vertex, and returns the absolute
    * value of the data on the vertex.
    * \code
    * float absolute_vertex_data(const graph_type::vertex_type& vertex) {
    *   return std::fabs(vertex.data());
    * }
    * \endcode
    * After which calling:
    * \code
    * float sum = graph.map_reduce_vertices<float>(absolute_vertex_data);
    * \endcode
    * will call the <code>absolute_vertex_data()</code> function
    * on each vertex in the graph. <code>absolute_vertex_data()</code>
    * reads the value of the vertex and returns the absolute result.
    * This return values are then summed together and returned.
    * All machines see the same result.
    *
    * The template argument <code><float></code> is needed to inform
    * the compiler regarding the return type of the mapfunction.
    *
    * The optional argument vset can be used to restrict he set of vertices
    * map-reduced over.
    *
    * ### Relations
    * This function is similar to
    * graphlab::iengine::map_reduce_vertices()
    * with the difference that this does not take a context
    * and thus cannot influence engine signalling.
    * transform_vertices() can be used to perform a similar
    * but may also make modifications to graph data.
    *
    * \tparam ReductionType The output of the map function. Must have
    *                    operator+= defined, and must be \ref sec_serializable.
    * \tparam VertexMapperType The type of the map function.
    *                          Not generally needed.
    *                          Can be inferred by the compiler.
    * \param mapfunction The map function to use. Must take
    *                   a \ref vertex_type, or a reference to a
    *                   \ref vertex_type as its only argument.
    *                   Returns a ReductionType which must be summable
    *                   and \ref sec_serializable .
    * \param vset The set of vertices to map reduce over. Optional. Defaults to
    *             complete_set()
    */
    template <typename GraphType, typename ReductionType, typename MapFunctionType>
    ReductionType map_reduce_vertices(GraphType& g,
                                      MapFunctionType mapfunction,
                                      const vertex_set& vset = GraphType::complete_set()) {
      BOOST_CONCEPT_ASSERT((graphlab::Serializable<ReductionType>));
      BOOST_CONCEPT_ASSERT((graphlab::OpPlusEq<ReductionType>));
      typedef typename GraphType::vertex_type vertex_type;

      if(!g.is_finalized()) {
        logstream(LOG_FATAL)
          << "\n\tAttempting to run graph.map_reduce_vertices(...) "
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
          auto lvertex = g.l_vertex(i);
          if (lvertex.owned() && vset.l_contains(lvid_type(i))) {
            if (!result_set) {
              vertex_type vtx(lvertex);
              result = mapfunction(vtx);
              result_set = true;
            }
            else if (result_set){
              const vertex_type vtx(lvertex);
              const ReductionType tmp = mapfunction(vtx);
              result += tmp;
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
    } // end of map_reduce_vertices
} // graphlab
#endif

