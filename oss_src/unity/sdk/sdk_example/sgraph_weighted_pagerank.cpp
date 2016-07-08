/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

/**
 * @author Jay Gu (jgu@turi.com)
 *
 * @date November 10, 2014
 *
 * @brief SGraph SDK Example
 *
 * This example produces the extension function: weighted_pagerank
 *
 * To run this example, make a shared library and import into python.
 *
 * ### Compilation
 * To compile, run
 * \code
 * cd graphlab-sdk
 * make
 * \endcode
 *
 * ### Usage
 *
 * In python or ipython, first import graphlab, then import the extension
 * so file. Now you have access to the two extension functions in the
 * sgraph_example module as regular python function.
 *
 * Example:
 * \code{.py}
 * import graphlab
 * from sdk_example.sgraph_weighted_pagerank import weighted_pagerank
 *
 * # The higgs-twitter graph is from Stanford SNAP project. (http//snap.stanford.edu)
 * url = 'http://snap.stanford.edu/data/higgs-reply_network.edgelist.gz'
 * g = graphlab.load_sgraph(url, format='snap', delimiter=' ')
 *
 * g2 = weighted_pagerank(g, num_iterations=10, weight_field='X3')
 * print g2.vertices.sort('pagerank', ascending=False)
 *
 * \endcode
 *
 * Produces output
 * \code{.txt}
 *
 * Top 10 vertices with highest weighted pagrank in g
 * +--------+---------------+
 * |  __id  |    pagerank   |
 * +--------+---------------+
 * | 45848  | 326.808929483 |
 * | 28423  | 138.143053733 |
 * | 53134  | 67.5302627568 |
 * | 56576  | 58.2142161056 |
 * | 77095  | 49.0152734282 |
 * | 421960 | 49.0152734282 |
 * | 203739 | 49.0152734282 |
 * | 344282 | 49.0152734282 |
 * | 48053  | 49.0152734282 |
 * | 56437  | 49.0152734282 |
 * |  ...   |      ...      |
 * +--------+---------------+
 */

#include <string>
#include <vector>
#include <map>
#include <graphlab/sdk/toolkit_function_macros.hpp>
#include <graphlab/sdk/toolkit_class_macros.hpp>
#include <graphlab/flexible_type/flexible_type.hpp>
#include <graphlab/sdk/gl_sframe.hpp>
#include <graphlab/sdk/gl_sgraph.hpp>

using namespace graphlab;

/**
 * Compute weighted pagerank of the given graph.
 *
 * Return a new graph with "pagerank" field on each vertex.
 *
 * \param g The input graph
 * \param iters Number of iterations
 * \param weighted_field The edge field which contains the weight value.
 */
gl_sgraph weighted_pagerank(const gl_sgraph& g, size_t iters, const std::string& weight_field) {
  const double RESET_PROB=0.15;

  // Get rid of unused fields.
  gl_sgraph g_min = g.select_fields({weight_field});

  // Count the total out going weight of each vertex into an SFrame.
  gl_sframe outgoing_weight = g_min.get_edges().groupby({"__src_id"}, {{"total_weight", aggregate::SUM(weight_field)}});

  // Add the total_weight to the graph as vertex atrribute.
  // We can update the vertex data by adding the same vertex.
  gl_sgraph g2 = g_min.add_vertices(outgoing_weight, "__src_id");

  // Lambda function for triple_apply
  auto pr_update = [&weight_field](edge_triple& triple)->void {
    double weight = triple.edge[weight_field];
    triple.target["pagerank"] += triple.source["pagerank_prev"] * weight / triple.source["total_weight"];
  };

  // Initialize pagerank value
  g2.vertices()["pagerank_prev"] = 1.0;

  // Iteratively run triple_apply with the update function
  for (size_t i = 0; i < iters; ++i) {

    g2.vertices()["pagerank"] = 0.0;

    logprogress_stream << "Iteration " << (i+1) << std::endl;
    g2 = g2.triple_apply(pr_update, {"pagerank"});

    g2.vertices()["pagerank"] = RESET_PROB + (1-RESET_PROB) * g2.vertices()["pagerank"];
    g2.vertices()["pagerank_prev"] = g2.vertices()["pagerank"];
  }

  g2.vertices().remove_column("pagerank_prev");
  g2.vertices().remove_column("total_weight");
  return g2;
}

BEGIN_FUNCTION_REGISTRATION
REGISTER_FUNCTION(weighted_pagerank, "graph", "num_iterations", "weight_field");
END_FUNCTION_REGISTRATION
