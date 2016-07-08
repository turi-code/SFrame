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
 * This example produces the extension function: edge_deduplication
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
 * from sdk_example.sgraph_edge_deduplication import edge_deduplication
 *
 * # The higgs-twitter graph is from Stanford SNAP project. (http//snap.stanford.edu)
 * url = 'http://snap.stanford.edu/data/higgs-reply_network.edgelist.gz'
 * g = graphlab.load_sgraph(url, format='snap', delimiter=' ')
 *
 * # Manually duplicate some edges
 * g2 = g.add_edges(g.edges.sample(0.1))
 * print g2.summary()
 * g3 = sgraph_example.edge_deduplication(g2)
 * print g3.summary()
 * \endcode
 *
 * Produces output
 * \code{.txt}
 *
 * Summary of g2
 * SGraph({'num_edges': 33316, 'num_vertices': 37366})
 * Vertex Fields:['__id', 'pagerank']
 * Edge Fields:['X3', '__dst_id', '__src_id']
 *
 * Summary of g3
 * SGraph({'num_edges': 30836, 'num_vertices': 37366})
 * Vertex Fields:['__id', 'pagerank']
 * Edge Fields:['X3', '__dst_id', '__src_id']
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
 * Return a new graph with no duplicate edges.
 * 
 * \param g The input graph
 */
gl_sgraph edge_deduplication(const gl_sgraph& g) {
  std::vector<std::string> edge_fields = g.get_edge_fields();

  std::map<std::string, aggregate::groupby_descriptor_type> groupby_operators;
  for (const auto& f: edge_fields) {
    if (f != "__src_id" && f != "__dst_id") {
      groupby_operators[f] = aggregate::SELECT_ONE(f);
    }
  }
  gl_sframe edge_dedup = g.get_edges().groupby( {"__src_id", "__dst_id"}, groupby_operators );
  return gl_sgraph(g.get_vertices(), edge_dedup);
}

BEGIN_FUNCTION_REGISTRATION
REGISTER_FUNCTION(edge_deduplication, "graph");
END_FUNCTION_REGISTRATION
