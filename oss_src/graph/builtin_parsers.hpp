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
#ifndef GRAPHLAB_GRAPH_BUILTIN_PARSERS_HPP
#define GRAPHLAB_GRAPH_BUILTIN_PARSERS_HPP

#include <string>
#include <sstream>
#include <iostream>

#if defined(__cplusplus) && __cplusplus >= 201103L
// do not include spirit
#else
#include <boost/config/warning_disable.hpp>
#include <boost/spirit/include/qi.hpp>
#include <boost/spirit/include/phoenix_core.hpp>
#include <boost/spirit/include/phoenix_operator.hpp>
#include <boost/spirit/include/phoenix_stl.hpp>
#endif

#include <graph/graph_basic_types.hpp>
#include <util/stl_util.hpp>
#include <logger/logger.hpp>
#include <serialization/serialization_includes.hpp>
#include <graphlab/util/charstream.hpp>



namespace graphlab {

  namespace builtin_parsers {
  
    /**
     * \brief Parse files in the Stanford Network Analysis Package format.
     *
     * example:
     *
     *  # some comment
     *  # another comment
     *  1 2
     *  3 4
     *  1 4
     *
     */
    template <typename Graph>
    bool snap_parser(Graph& graph, const std::string& srcfilename,
                     const std::string& str) {
      if (str.empty()) return true;
      else if (str[0] == '#') {
        std::cerr << str << std::endl;
      } else {
        size_t source, target;
        char* targetptr;
        source = strtoul(str.c_str(), &targetptr, 10);
        if (targetptr == NULL) return false;
        target = strtoul(targetptr, NULL, 10);
        if(source != target) graph.add_edge(source, target);
      }
      return true;
    } // end of snap parser

    /**
     * \brief Parse files in the standard tsv format
     *
     * This is identical to the SNAP format but does not allow comments.
     *
     */
    template <typename Graph>
    bool tsv_parser(Graph& graph, const std::string& srcfilename,
                    const std::string& str) {
      if (str.empty()) return true;
      size_t source, target;
      char* targetptr;
      source = strtoul(str.c_str(), &targetptr, 10);
      if (targetptr == NULL) return false;
      target = strtoul(targetptr, NULL, 10);
      if(source != target) graph.add_edge(source, target);
      return true;
    } // end of tsv parser


    template <typename Graph>
    bool csv_parser(Graph& graph, 
          const std::string& filename, 
          const std::string& textline) {
      if (textline.empty()) return true;
      size_t split = textline.find_first_of(",");
      if (split == std::string::npos) return true;
      else {
        std::string t = textline;
        t[split] = 0;
        graph.add_edge(strtoul(t.c_str(), NULL, 10), 
            strtoul(t.c_str() + split + 1, NULL, 10));
        return true;
      }
    }


#if defined(__cplusplus) && __cplusplus >= 201103L
    // The spirit parser seems to have issues when compiling under
    // C++11. Temporary workaround with a hard coded parser. TOFIX
    template <typename Graph>
    bool adj_parser(Graph& graph, const std::string& srcfilename,
                    const std::string& line) {
      // If the line is empty simply skip it
      if(line.empty()) return true;
      std::stringstream strm(line);
      vertex_id_type source; 
      size_t n;
      strm >> source;
      if (strm.fail()) return false;
      strm >> n;
      if (strm.fail()) return true;

      size_t nadded = 0;
      while (strm.good()) {
        vertex_id_type target;
        strm >> target;
        if (strm.fail()) break;
        if (source != target) graph.add_edge(source, target);
        ++nadded;
      } 
      if (n != nadded) return false;
      return true;
    } // end of adj parser

#else

    template <typename Graph>
    bool adj_parser(Graph& graph, const std::string& srcfilename,
                    const std::string& line) {
      // If the line is empty simply skip it
      if(line.empty()) return true;
      // We use the boost spirit parser which requires (too) many separate
      // namespaces so to make things clear we shorten them here.
      namespace qi = boost::spirit::qi;
      namespace ascii = boost::spirit::ascii;
      namespace phoenix = boost::phoenix;
      vertex_id_type source(-1);
      vertex_id_type ntargets(-1);
      std::vector<vertex_id_type> targets;
      const bool success = qi::phrase_parse
        (line.begin(), line.end(),       
         //  Begin grammar
         (
          qi::ulong_[phoenix::ref(source) = qi::_1] >> -qi::char_(",") >>
          qi::ulong_[phoenix::ref(ntargets) = qi::_1] >> -qi::char_(",") >>
          *(qi::ulong_[phoenix::push_back(phoenix::ref(targets), qi::_1)] % -qi::char_(","))
          )
         ,
         //  End grammar
         ascii::space); 
      // Test to see if the boost parser was able to parse the line
      if(!success || ntargets != targets.size()) {
        logstream(LOG_ERROR) << "Parse error in vertex prior parser." << std::endl;
        return false;
      }
      for(size_t i = 0; i < targets.size(); ++i) {
        if(source != targets[i]) graph.add_edge(source, targets[i]);
      }
      return true;
    } // end of adj parser
#endif

    template <typename Graph>
    struct tsv_writer{
      typedef typename Graph::vertex_type vertex_type;
      typedef typename Graph::edge_type edge_type;
      std::string save_vertex(vertex_type) { return ""; }
      std::string save_edge(edge_type e) {
        return tostr(e.source().id()) + "\t" + tostr(e.target().id()) + "\n";
      }
    };



    
    template <typename Graph>
    struct graphjrl_writer{
      typedef typename Graph::vertex_type vertex_type;
      typedef typename Graph::edge_type edge_type;

      /**
       * \internal
       * Replaces \255 with \255\1
       * Replaces \\n with \255\0
       */
      static std::string escape_newline(charstream& strm) {
        size_t ctr = 0;
        char *ptr = strm->c_str();
        size_t strmlen = strm->size();
        for (size_t i = 0;i < strmlen; ++i) {
          ctr += (ptr[i] == (char)255 || ptr[i] == '\n');
        }

        std::string ret(ctr + strmlen, 0);

        size_t target = 0;
        for (size_t i = 0;i < strmlen; ++i, ++ptr) {
          if ((*ptr) == (char)255) {
            ret[target] = (char)255;
            ret[target + 1] = 1;
            target += 2;
          }
          else if ((*ptr) == '\n') {
            ret[target] = (char)255;
            ret[target + 1] = 0;
            target += 2;
          }
          else {
            ret[target] = (*ptr);
            ++target;
          }
        }
        assert(target == ctr + strmlen);
        return ret;
      }

      /**
       * \internal
       * Replaces \255\1 with \255
       * Replaces \255\0 with \\n
       */
      static std::string unescape_newline(const std::string& str) {
        size_t ctr = 0;
        // count the number of escapes
        for (size_t i = 0;i < str.length(); ++i) {
          ctr += (str[i] == (char)255);
        }
        // real length is string length - escapes
        std::string ret(str.size() - ctr, 0);

        const char escapemap[2] = {'\n', (char)255};
        
        size_t target = 0;
        for (size_t i = 0;i < str.length(); ++i, ++target) {
          if (str[i] == (char)255) {
            // escape character
            ++i;
            ASSERT_MSG(str[i] == 0 || str[i] == 1,
                       "Malformed escape sequence in graphjrl file.");
            ret[target] = escapemap[(int)str[i]];
          }
          else {
            ret[target] = str[i];
          }
        }
        return ret;
      }
      
      std::string save_vertex(vertex_type v) {
        charstream strm(128);
        oarchive oarc(strm);
        oarc << char(0) << v.id() << v.data();
        strm.flush();
        return escape_newline(strm) + "\n";
      }
      
      std::string save_edge(edge_type e) {
        charstream strm(128);
        oarchive oarc(strm);
        oarc << char(1) << e.source().id() << e.target().id() << e.data();
        strm.flush();
        return escape_newline(strm) + "\n";
      }
    };



    template <typename Graph>
    bool graphjrl_parser(Graph& graph, const std::string& srcfilename,
                    const std::string& str) {
      std::string unescapedstr = graphjrl_writer<Graph>::unescape_newline(str);
      boost::iostreams::stream<boost::iostreams::array_source>
                      istrm(unescapedstr.c_str(), unescapedstr.length());
      iarchive iarc(istrm);
      
      char entrytype;
      iarc >> entrytype;
      if (entrytype == 0) {
        typename Graph::vertex_id_type vid;
        typename Graph::vertex_data_type vdata;
        iarc >> vid >> vdata;
        graph.add_vertex(vid, vdata);
      }
      else if (entrytype == 1) {
        typename Graph::vertex_id_type srcvid, destvid;
        typename Graph::edge_data_type edata;
        iarc >> srcvid >> destvid >> edata;
        graph.add_edge(srcvid, destvid, edata);
      }
      else {
        return false;
      }
      return true;
    }
    
  } // namespace builtin_parsers
} // namespace graphlab

#endif
