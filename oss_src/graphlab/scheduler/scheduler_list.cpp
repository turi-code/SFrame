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


#include <algorithm>
#include <graphlab/scheduler/scheduler_list.hpp>
#include <util/stl_util.hpp>


namespace graphlab {
  
  std::vector<std::string> get_scheduler_names() {
    std::vector<std::string> ret;
#define __APPEND_TO_RET__(r_unused, data_unused, i,  elem)      \
    ret.push_back(BOOST_PP_TUPLE_ELEM(3,0,elem));
    BOOST_PP_SEQ_FOR_EACH_I(__APPEND_TO_RET__, _, __SCHEDULER_LIST__)
#undef __APPEND_TO_RET__
      return ret;
  }


  std::string get_scheduler_names_str() {
    std::string ret;
    std::vector<std::string> schednames;
    schednames = get_scheduler_names();
    for (size_t i = 0; i < schednames.size(); ++i) {
      if (i > 0) {
        ret = ret + ", ";
      }
      ret = ret + schednames[i];
    }
    return ret;
  }

  static std::string add_line_breaks(const std::string &s, size_t numcols) {
    size_t pos = 0;
    std::string ret;
    while(pos < s.length() - 1) {
      size_t oldpos = pos;
      pos = std::min(pos + numcols, s.length());
    
      size_t newpos = pos;
      // search backward for a space if we are not at the end of the
      // string
      if (pos < s.length()) {
        newpos = s.rfind(" ", pos);
      }
      // if we get back to the old position, or we fail to find a
      // space, force the break
      if (newpos  == std::string::npos || newpos == oldpos) {
        newpos = pos;
      }
      // break
      ret = ret + trim(s.substr(oldpos, newpos - oldpos)) + "\n";
      pos = newpos;
    }
    return ret;
  }


  void print_scheduler_info(std::string s, std::ostream &out) {
    typedef char dummy_message_type;     
    // this is annoying... I need to instantiate the graph<char, char> type to
    // even call the scheduler
#define __GENERATE_SCHEDULER_HELP__(r_unused, data_unused, i,  elem)    \
    BOOST_PP_EXPR_IF(i, else) if (s == BOOST_PP_TUPLE_ELEM(3,0,elem)) { \
      out << "\n";                                                      \
      out << BOOST_PP_TUPLE_ELEM(3,0,elem) << " scheduler\n";           \
      out << std::string(50, '-') << std::endl;                         \
      out << add_line_breaks(BOOST_PP_TUPLE_ELEM(3,2,elem), 50) << "\n" \
          << "Options: \n";                                             \
      BOOST_PP_TUPLE_ELEM(3,1,elem)                                     \
        ::print_options_help(out);                                      \
    }
    /*
     * if (scheduler == "sweep") {
     *   sweep_scheduler<graph<char,char> >::print_options_help(out);
     * }
     * ...
     */
    // generate the construction calls
    BOOST_PP_SEQ_FOR_EACH_I(__GENERATE_SCHEDULER_HELP__, _, __SCHEDULER_LIST__)
    else {
      out << "Scheduler " << s << " not found" << "\n";
    }
#undef __GENERATE_SCHEDULER_HELP__
  } // end of print scheduler info


} // end of namespace graphlab



