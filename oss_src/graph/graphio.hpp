/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_GRAPH_IO_HPP
#define GRAPHLAB_GRAPH_IO_HPP

#include <logger/logger.hpp>
#include <boost/functional.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/iostreams/stream.hpp>
#include <boost/iostreams/filtering_streambuf.hpp>

#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/copy.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/filesystem.hpp>

#include <graph/builtin_parsers.hpp>
#include <parallel/lambda_omp.hpp>
#include <fileio/general_fstream.hpp>
#include <graphlab/util/fs_util.hpp>
namespace graphlab {

  namespace io {
    /** \brief Load a distributed graph from a native binary format
     * previously saved with save_binary(). This function must be called
     *  simultaneously on all machines.
     *
     * This function loads a sequence of files numbered
     * \li [prefix].0.gz
     * \li [prefix].1.gz
     * \li [prefix].2.gz
     * \li etc.
     *
     * These files must be previously saved using save_binary(), and
     * must be saved <b>using the same number of machines</b>.
     * This function uses the graphlab serialization system, so
     * the user must ensure that the vertex data and edge data
     * serialization formats have not changed since the graph was saved.
     *
     * A graph loaded using load_binary() is already finalized and
     * structure modifications are not permitted after loading.
     *
     * Return true on success and false on failure if the file cannot be loaded.
     */
    template <typename GraphType>
    bool load_binary(GraphType& g, const std::string& prefix) {
      g.dc().full_barrier();
      std::string fname = prefix + tostr(g.procid()) + ".bin";

      logstream(LOG_INFO) << "Load graph from " << fname << std::endl;
      general_ifstream fin(fname, true);
      if(!fin.good()) {
        logstream(LOG_ERROR) << "\n\tError opening file: " << fname << std::endl;
        return false;
      }
      iarchive iarc(fin);
      iarc >> g;
      logstream(LOG_INFO) << "Finish loading graph from " << fname << std::endl;
      g.dc().full_barrier();
      return true;
    } // end of load


    /** \brief Saves a distributed graph to a native binary format
     * which can be loaded with load_binary(). This function must be called
     *  simultaneously on all machines.
     *
     * This function saves a sequence of files numbered
     * \li [prefix].0.gz
     * \li [prefix].1.gz
     * \li [prefix].2.gz
     * \li etc.
     *
     * This files can be loaded with load_binary() using the <b> same number
     * of machines</b>.
     * This function uses the graphlab serialization system, so
     * the vertex data and edge data serialization formats must not
     * change between the use of save_binary() and load_binary().
     *
     * If the graph is not alreasy finalized before save_binary() is called,
     * this function will finalize the graph.
     *
     * Returns true on success, and false if the graph cannot be loaded from
     * the specified file.
     */
    template<typename GraphType>
    bool save_binary(const GraphType& g, const std::string& prefix) {
      g.dc().full_barrier();
      ASSERT_TRUE (g.is_finalized());
      timer savetime;  savetime.start();
      std::string fname = prefix + tostr(g.procid()) + ".bin";
      logstream(LOG_INFO) << "Save graph to " << fname << std::endl;

      general_ofstream fout(fname, true);
      if (!fout.good()) {
        logstream(LOG_ERROR) << "\n\tError opening file: " << fname << std::endl;
        return false;
      }
      oarchive oarc(fout);
      oarc << g;
      logstream(LOG_INFO) << "Finish saving graph to " << fname << std::endl
                          << "Finished saving binary graph: "
                          << savetime.current_time() << std::endl;
      g.dc().full_barrier();
      fout.close();
      return true;
    } // end of save




    template <typename GraphType, typename VertexFunctorType>
    void parallel_for_vertices(GraphType& g,
                               std::vector<VertexFunctorType>& accfunction);
    template <typename GraphType, typename EdgeFunctorType>
    void parallel_for_edges(GraphType& g,
                            std::vector<EdgeFunctorType>& accfunction);

    /**
     * \brief Saves the graph to the filesystem using a provided Writer object.
     * Like \ref save(const std::string& prefix, writer writer, bool gzip, bool save_vertex, bool save_edge, size_t files_per_machine) "save()"
     * but only saves to local filesystem.
     */
    template<typename GraphType, typename Writer>
    void save(GraphType& g,
              const std::string& prefix,
              Writer writer,
              bool gzip = true,
              bool save_vertex = true,
              bool save_edge = true,
              size_t files_per_machine = 4) {
      typedef typename GraphType::vertex_type vertex_type;
      typedef typename GraphType::edge_type edge_type;
      typedef std::function<void(vertex_type)> vertex_function_type;
      typedef std::function<void(edge_type)> edge_function_type;
      typedef boost::iostreams::filtering_stream<boost::iostreams::output>
          boost_fstream_type;

      if(!g.is_finalized()) {
        g.finalize();
      }

      g.dc().full_barrier();
      // figure out the filenames
      std::vector<std::string> graph_files;
      std::vector<union_fstream*> outstreams;
      std::vector<boost_fstream_type*> booststreams;

      graph_files.resize(files_per_machine);
      for(size_t i = 0; i < files_per_machine; ++i) {
        graph_files[i] = prefix + "_" + tostr(1 + i + g.procid() * files_per_machine)
            + "_of_" + tostr(g.numprocs() * files_per_machine);
        if (gzip) graph_files[i] += ".gz";
      }

      // create the vector of callbacks
      std::vector<vertex_function_type> vertex_callbacks(graph_files.size());
      std::vector<edge_function_type> edge_callbacks(graph_files.size());

      for(size_t i = 0; i < graph_files.size(); ++i) {
        logstream(LOG_INFO) << "Saving to file: " << graph_files[i] << std::endl;
        union_fstream* out_file =
            new union_fstream(graph_files[i], std::ios_base::out | std::ios_base::binary);
        // attach gzip if the file is gzip
        boost_fstream_type* fout = new boost_fstream_type;
        // Using gzip filter
        if (gzip) fout->push(boost::iostreams::gzip_compressor());
        fout->push(*(out_file->get_ostream()));
        outstreams.push_back(out_file);
        booststreams.push_back(fout);

        vertex_callbacks[i] = [&](vertex_type v) {*fout << writer.save_vertex(v);};
        edge_callbacks[i] = [&](edge_type e) {*fout << writer.save_edge(e);};
      }

      if (save_vertex) parallel_for_vertices(g, vertex_callbacks);
      if (save_edge) parallel_for_edges(g, edge_callbacks);

      // cleanup
      for(size_t i = 0; i < graph_files.size(); ++i) {
        booststreams[i]->pop();
        if (gzip) booststreams[i]->pop();
        delete booststreams[i];
        delete outstreams[i];
      }
      vertex_callbacks.clear();
      edge_callbacks.clear();
      outstreams.clear();
      booststreams.clear();
      g.dc().full_barrier();
    } // end of save to posixfs

    template<typename GraphType>
    void save_format(GraphType& g, 
                     const std::string& prefix, const std::string& format,
                     bool gzip = true, size_t files_per_machine = 4) {
      if (prefix.length() == 0)
        return;
      if (format == "snap" || format == "tsv") {
        save(g, prefix, builtin_parsers::tsv_writer<GraphType>(),
             gzip, false, true, files_per_machine);
      } else if (format == "graphjrl") {
        save(g, prefix, builtin_parsers::graphjrl_writer<GraphType>(),
             gzip, true, true, files_per_machine);
      } else if (format == "bin") {
        save_binary(g, prefix);
      } 
      // else if (format == "bintsv4") {
      //   save_direct(prefix, gzip, &graph_type::save_bintsv4_to_stream);
      // }
      else {
        logstream(LOG_ERROR)
            << "Unrecognized Format \"" << format << "\"!" << std::endl;
        throw(std::string("Unrecognized Format \"" + format + "\""));
        return;
      }
    } // end of save structure





    template <typename GraphType> using line_parser_type = 
        boost::function<bool(GraphType&, const std::string&,
                             const std::string&)>;

    /**
       \internal
       This internal function is used to load a single line from an input stream
     */
    template<typename GraphType, typename Fstream>
    bool load_from_stream(GraphType& g, std::string filename, Fstream& fin,
                          line_parser_type<GraphType>& line_parser) {
      size_t linecount = 0;
      timer ti; ti.start();
      while(fin.good() && !fin.eof()) {
        std::string line;
        std::getline(fin, line);
        if(line.empty()) continue;
        if(fin.fail()) break;
        const bool success = line_parser(g, filename, line);
        if (!success) {
          logstream(LOG_WARNING)
              << "Error parsing line " << linecount << " in "
              << filename << ": " << std::endl
              << "\t\"" << line << "\"" << std::endl;
          return false;
        }
        ++linecount;
        if (ti.current_time() > 5.0) {
          logstream(LOG_INFO) << linecount << " Lines read" << std::endl;
          ti.start();
        }
      }
      return true;
    } // end of load from stream



    /**
     *  \brief Load a graph from a collection of files in stored on
     *  the filesystem using the user defined line parser. Like
     *  \ref load(const std::string& path, line_parser_type line_parser)
     *  but only loads from the filesystem.
     */
    template<typename GraphType>
    void load(GraphType& g, std::string prefix,
              line_parser_type<GraphType> line_parser) {
      if (prefix.length() == 0)
        return;
      g.dc().full_barrier();
      g.clear();
      std::string directory_name; std::string original_path(prefix);
      boost::filesystem::path path(prefix);
      std::string search_prefix;
      if (boost::filesystem::is_directory(path)) {
        // if this is a directory
        // force a "/" at the end of the path
        // make sure to check that the path is non-empty. (you do not
        // want to make the empty path "" the root path "/" )
        directory_name = path.native();
      } else {
        directory_name = path.parent_path().native();
        search_prefix = path.filename().native();
        directory_name = (directory_name.empty() ? "." : directory_name);
      }
      std::vector<std::string> graph_files;
      fs_util::list_files_with_prefix(directory_name, search_prefix, graph_files);
      if (graph_files.size() == 0) {
        logstream(LOG_WARNING) << "No files found matching " << original_path << std::endl;
      }

      parallel_for(0, graph_files.size(), [&](size_t i) {
        if (i % g.numprocs() == g.procid()) {
          logstream(LOG_EMPH) << "Loading graph from file: " << graph_files[i] << std::endl;
          general_ifstream fin(graph_files[i]);
          if(!fin.good()) {
            log_and_throw_io_failure("Cannot open file: " + graph_files[i]);
          }
          const bool success = load_from_stream(g, graph_files[i], fin, line_parser);
          if(!success) {
            log_and_throw_io_failure("Fail parsing file: " + graph_files[i]);
          }
        }
      });
      g.dc().full_barrier();
      g.finalize();
    } // end of load

    /**
     *  \brief load a graph with a standard format. Must be called on all
     *  machines simultaneously.
     *
     *  The supported graph formats are described in \ref graph_formats.
     */
    template<typename GraphType>
    void load_format(GraphType& g,
                     const std::string& path,
                     const std::string& format) {
      line_parser_type<GraphType> line_parser;
      if (path.length() == 0)
        return;
      if (format == "snap") {
        line_parser = builtin_parsers::snap_parser<GraphType>;
        load(g, path, line_parser);
      } else if (format == "adj") {
        line_parser = builtin_parsers::adj_parser<GraphType>;
        load(g, path, line_parser);
      } else if (format == "tsv") {
        line_parser = builtin_parsers::tsv_parser<GraphType>;
        load(g, path, line_parser);
      } else if (format == "csv") {
        line_parser = builtin_parsers::csv_parser<GraphType>;
        load(g, path, line_parser);
      } else if (format == "graphjrl") {
        line_parser = builtin_parsers::graphjrl_parser<GraphType>;
        load(g, path, line_parser);
        // } else if (format == "bintsv4") {
        //    load_direct(path,&graph_type::load_bintsv4_from_stream);
      } else if (format == "bin") {
        load_binary(g, path);
      } else {
        logstream(LOG_ERROR)
            << "Unrecognized Format \"" << format << "\"!" << std::endl;
        return;
      }
    } // end of load


    // disable documentation for parallel_for stuff. These are difficult
    // to use properly by the user
    /// \cond GRAPHLAB_INTERNAL
    /**
     * \internal
     * parallel_for_vertices will partition the set of vertices among the
     * vector of accfunctions. Each accfunction is then executed sequentially
     * on the set of vertices it was assigned.
     *
      * \param accfunction must be a void function which takes a single
      * vertex_type argument. It may be a functor and contain state.
      * The function need not be reentrant as it is only called sequentially
     */
    template <typename GraphType, typename VertexFunctorType>
    void parallel_for_vertices(GraphType& g,
                               std::vector<VertexFunctorType>& accfunction) {
      typedef typename GraphType::vertex_type vertex_type;
      ASSERT_TRUE(g.is_finalized());
      g.dc().barrier();
      int numaccfunctions = (int)accfunction.size();
      ASSERT_GE(numaccfunctions, 1);
#ifdef _OPENMP
      #pragma omp parallel for
#endif
      for (int i = 0; i < (int)accfunction.size(); ++i) {
        for (int j = i;j < (int)g.num_local_vertices(); j+=numaccfunctions) {
          auto lvertex = g.l_vertex(j);
          if (lvertex.owned()) {
            accfunction[i](vertex_type(lvertex));
          }
        }
      }
      g.dc().barrier();
    }


    /**
     * \internal
     * parallel_for_edges will partition the set of edges among the
     * vector of accfunctions. Each accfunction is then executed sequentially
     * on the set of edges it was assigned.
     *
     * \param accfunction must be a void function which takes a single
     * edge_type argument. It may be a functor and contain state.
     * The function need not be reentrant as it is only called sequentially
     */
    template <typename GraphType, typename EdgeFunctorType>
    void parallel_for_edges(GraphType& g,
                            std::vector<EdgeFunctorType>& accfunction) {
      typedef typename GraphType::local_edge_type local_edge_type;
      typedef typename GraphType::edge_type edge_type;
      ASSERT_TRUE(g.is_finalized());
      g.dc().barrier();
      int numaccfunctions = (int)accfunction.size();
      ASSERT_GE(numaccfunctions, 1);
#ifdef _OPENMP
      #pragma omp parallel for
#endif
      for (int i = 0; i < (int)accfunction.size(); ++i) {
        for (int j = i;j < (int)g.num_local_vertices(); j+=numaccfunctions) {
          for (const local_edge_type& e : g.l_vertex(j).in_edges()) {
            accfunction[i](edge_type(e));
          }
        }
      }
      g.dc().barrier();
    }
  } // end of namespace io
} // end of namespace graphlab


//     void save_bintsv4_to_stream(std::ostream& out) {
//       for (int i = 0; i < (int)local_graph.num_vertices(); ++i) {
//         uint32_t src = l_vertex(i).global_id();
//         foreach(local_edge_type e, l_vertex(i).out_edges()) {
//           uint32_t dest = e.target().global_id();
//           out.write(reinterpret_cast<char*>(&src), 4);
//           out.write(reinterpret_cast<char*>(&dest), 4);
//         }
//         if (l_vertex(i).owner() == rpc.procid()) {
//           vertex_type gv = vertex_type(l_vertex(i));
//           // store disconnected vertices if I am the master of the vertex
//           if (gv.num_in_edges() == 0 && gv.num_out_edges() == 0) {
//             out.write(reinterpret_cast<char*>(&src), 4);
//             uint32_t dest = (uint32_t)(-1);
//             out.write(reinterpret_cast<char*>(&dest), 4);
//           }
//         }
//       }
//     }
// 
//     bool load_bintsv4_from_stream(std::istream& in) {
//       while(in.good()) {
//         uint32_t src, dest;
//         in.read(reinterpret_cast<char*>(&src), 4);
//         in.read(reinterpret_cast<char*>(&dest), 4);
//         if (in.fail()) break;
//         if (dest == (uint32_t)(-1)) {
//           add_vertex(src);
//         }
//         else {
//           add_edge(src, dest);
//         }
//       }
//       return true;
//     }
// 
// 
//     /** \brief Saves a distributed graph using a direct ostream saving function
//      *
//      * This function saves a sequence of files numbered
//      * \li [prefix]_0
//      * \li [prefix]_1
//      * \li [prefix]_2
//      * \li etc.
//      *
//      * This files can be loaded with direct_stream_load().
//      */
//     void save_direct(const std::string& prefix, bool gzip,
//                     boost::function<void (graph_type*, std::ostream&)> saver) {
//       rpc.full_barrier();
//       finalize();
//       timer savetime;  savetime.start();
//       std::string fname = prefix + "_" + tostr(rpc.procid() + 1) + "_of_" +
//                           tostr(rpc.numprocs());
//       if (gzip) fname = fname + ".gz";
//       logstream(LOG_INFO) << "Save graph to " << fname << std::endl;
//       if(boost::starts_with(fname, "hdfs://")) {
//         graphlab::hdfs hdfs;
//         graphlab::hdfs::fstream out_file(hdfs, fname, true);
//         boost::iostreams::filtering_stream<boost::iostreams::output> fout;
//         if (gzip) fout.push(boost::iostreams::gzip_compressor());
//         fout.push(out_file);
//         if (!fout.good()) {
//           logstream(LOG_FATAL) << "\n\tError opening file: " << fname << std::endl;
//           exit(-1);
//         }
//         saver(this, boost::ref(fout));
//         fout.pop();
//         if (gzip) fout.pop();
//         out_file.close();
//       } else {
//         std::ofstream out_file(fname.c_str(),
//                                std::ios_base::out | std::ios_base::binary);
//         if (!out_file.good()) {
//           logstream(LOG_FATAL) << "\n\tError opening file: " << fname << std::endl;
//           exit(-1);
//         }
//         boost::iostreams::filtering_stream<boost::iostreams::output> fout;
//         if (gzip) fout.push(boost::iostreams::gzip_compressor());
//         fout.push(out_file);
//         saver(this, boost::ref(fout));
//         fout.pop();
//         if (gzip) fout.pop();
//         out_file.close();
//       }
//       logstream(LOG_INFO) << "Finish saving graph to " << fname << std::endl
//                           << "Finished saving bintsv4 graph: "
//                           << savetime.current_time() << std::endl;
//       rpc.full_barrier();
//     } // end of save
// 
// 
// 
//     /**
//      *  \brief Load a graph from a collection of files in stored on
//      *  the filesystem using the user defined line parser. Like
//      *  \ref load(const std::string& path, line_parser_type line_parser)
//      *  but only loads from the filesystem.
//      */
//     void load_direct_from_posixfs(std::string prefix,
//                            boost::function<bool (graph_type*, std::istream&)> parser) {
//       std::string directory_name; std::string original_path(prefix);
//       boost::filesystem::path path(prefix);
//       std::string search_prefix;
//       if (boost::filesystem::is_directory(path)) {
//         // if this is a directory
//         // force a "/" at the end of the path
//         // make sure to check that the path is non-empty. (you do not
//         // want to make the empty path "" the root path "/" )
//         directory_name = path.native();
//       }
//       else {
//         directory_name = path.parent_path().native();
//         search_prefix = path.filename().native();
//         directory_name = (directory_name.empty() ? "." : directory_name);
//       }
//       std::vector<std::string> graph_files;
//       fs_util::list_files_with_prefix(directory_name, search_prefix, graph_files);
//       if (graph_files.size() == 0) {
//         logstream(LOG_WARNING) << "No files found matching " << original_path << std::endl;
//       }
//       for(size_t i = 0; i < graph_files.size(); ++i) {
//         if (i % rpc.numprocs() == rpc.procid()) {
//           logstream(LOG_EMPH) << "Loading graph from file: " << graph_files[i] << std::endl;
//           // is it a gzip file ?
//           const bool gzip = boost::ends_with(graph_files[i], ".gz");
//           // open the stream
//           std::ifstream in_file(graph_files[i].c_str(),
//                                 std::ios_base::in | std::ios_base::binary);
//           // attach gzip if the file is gzip
//           boost::iostreams::filtering_stream<boost::iostreams::input> fin;
//           // Using gzip filter
//           if (gzip) fin.push(boost::iostreams::gzip_decompressor());
//           fin.push(in_file);
//           const bool success = parser(this, boost::ref(fin));
//           if(!success) {
//             logstream(LOG_FATAL)
//               << "\n\tError parsing file: " << graph_files[i] << std::endl;
//           }
//           fin.pop();
//           if (gzip) fin.pop();
//         }
//       }
//       rpc.full_barrier();
//     }
// 
//     /**
//      *  \brief Load a graph from a collection of files in stored on
//      *  the HDFS using the user defined line parser. Like
//      *  \ref load(const std::string& path, line_parser_type line_parser)
//      *  but only loads from HDFS.
//      */
//     void load_direct_from_hdfs(std::string prefix,
//                          boost::function<bool (graph_type*, std::istream&)> parser) {
//       // force a "/" at the end of the path
//       // make sure to check that the path is non-empty. (you do not
//       // want to make the empty path "" the root path "/" )
//       std::string path = prefix;
//       if (path.length() > 0 && path[path.length() - 1] != '/') path = path + "/";
//       if(!hdfs::has_hadoop()) {
//         logstream(LOG_FATAL)
//           << "\n\tAttempting to load a graph from HDFS but GraphLab"
//           << "\n\twas built without HDFS."
//           << std::endl;
//       }
//       hdfs& hdfs = hdfs::get_hdfs();
//       std::vector<std::string> graph_files;
//       graph_files = hdfs.list_files(path);
//       if (graph_files.size() == 0) {
//         logstream(LOG_WARNING) << "No files found matching " << prefix << std::endl;
//       }
//       for(size_t i = 0; i < graph_files.size(); ++i) {
//         if (i % rpc.numprocs() == rpc.procid()) {
//           logstream(LOG_EMPH) << "Loading graph from file: " << graph_files[i] << std::endl;
//           // is it a gzip file ?
//           const bool gzip = boost::ends_with(graph_files[i], ".gz");
//           // open the stream
//           graphlab::hdfs::fstream in_file(hdfs, graph_files[i]);
//           boost::iostreams::filtering_stream<boost::iostreams::input> fin;
//           if(gzip) fin.push(boost::iostreams::gzip_decompressor());
//           fin.push(in_file);
//           const bool success = parser(this, boost::ref(fin));
//           if(!success) {
//             logstream(LOG_FATAL)
//               << "\n\tError parsing file: " << graph_files[i] << std::endl;
//           }
//           fin.pop();
//           if (gzip) fin.pop();
//         }
//       }
//       rpc.full_barrier();
//     }
// 
//     void load_direct(std::string prefix,
//              boost::function<bool (graph_type*, std::istream&)> parser) {
//       rpc.full_barrier();
//       if(boost::starts_with(prefix, "hdfs://")) {
//         load_direct_from_hdfs(prefix, parser);
//       } else {
//         load_direct_from_posixfs(prefix, parser);
//       }
//       rpc.full_barrier();
//     } // end of load

#endif
