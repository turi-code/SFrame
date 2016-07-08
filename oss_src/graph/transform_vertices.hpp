/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_TRANSFORM_VERTICES_HPP
#define GRAPHLAB_TRANSFORM_VERTICES_HPP

#include <graph/vertex_set.hpp>
#include <parallel/lambda_omp.hpp>

namespace graphlab {
  /**
   * \brief Performs a transformation operation on each vertex in the graph.
   *
   * Given a mapfunction, transform_vertices() calls mapfunction on
   * every vertex in graph. The map function may make modifications
   * to the data on the vertex. transform_vertices() must be called by all
   * machines simultaneously.
   *
   * The optional vset argument may be used to restrict the set of vertices
   * operated upon.
   *
   * ### Basic Usage
   * For instance, if the graph has integer vertex data, and integer edge
   * data:
   * \code
   *   typedef graphlab::distributed_graph<size_t, size_t> graph_type;
   * \endcode
   *
   * To set each vertex value to be the number of out-going edges,
   * we may write the following function:
   * \code
   * void set_vertex_value(graph_type::vertex_type& vertex)i {
   *   vertex.data() = vertex.num_out_edges();
   * }
   * \endcode
   *
   * Calling transform_vertices():
   * \code
   *   graph.transform_vertices(set_vertex_value);
   * \endcode
   * will run the <code>set_vertex_value()</code> function
   * on each vertex in the graph, setting its new value.
   *
   * ### Relations
   * map_reduce_vertices() provide similar signalling functionality,
   * but should not make modifications to graph data.
   * graphlab::iengine::transform_vertices() provide
   * the same graph modification capabilities, but with a context
   * and thus can perform signalling.
   *
   * \tparam VertexMapperType The type of the map function.
   *                          Not generally needed.
   *                          Can be inferred by the compiler.
   * \param mapfunction The map function to use. Must take an
   *                   \ref icontext_type& as its first argument, and
   *                   a \ref vertex_type, or a reference to a
   *                   \ref vertex_type as its second argument.
   *                   Returns void.
   * \param vset The set of vertices to transform. Optional. Defaults to
   *             complete_set()
   */
  template <typename GraphType, typename TransformType>
      void transform_vertices(GraphType& g,
                              TransformType transform_functor,
                              const vertex_set vset = GraphType::complete_set()) {
        typedef typename GraphType::vertex_type vertex_type;
        if(!g.is_finalized()) {
          logstream(LOG_FATAL)
              << "\n\tAttempting to call graph.transform_vertices(...)"
              << "\n\tbefore finalizing the graph."
              << std::endl;
        }
        g.dc().barrier();
        size_t ibegin = 0;
        size_t iend = g.num_local_vertices();
        parallel_for (ibegin, iend, [&](size_t i) {
            auto lvertex = g.l_vertex(i);
            if (lvertex.owned() && vset.l_contains(lvid_type(i))) {
              vertex_type vtx(lvertex);
              transform_functor(vtx);
            }
          });
        g.dc().barrier();
        g.synchronize();
      }
} // end of namespace
#endif
