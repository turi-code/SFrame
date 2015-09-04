/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_SGRAPH_IO_HPP
#define GRAPHLAB_SGRAPH_IO_HPP

#include<string>

class sgraph;

namespace graphlab {

  /**
   * Write the content of the graph into a json file.
   */
  void save_sgraph_to_json(const sgraph& g,
                           std::string targetfile);

  /**
   * Write the content of the graph into a collection csv files under target directory.
   * The vertex data are saved to vertex-groupid-partitionid.csv and edge data
   * are saved to edge-groupid-partitionid.csv.
   */
  void save_sgraph_to_csv(const sgraph& g,
                          std::string targetdir);
}

#endif
