/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
/*  
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
#include <graphlab/options/command_line_options.hpp>
#include <graphlab/scheduler/scheduler_list.hpp>


namespace boost {  

  template<>
  std::string lexical_cast< std::string>(const std::vector<int>& vec) {
    return graphlab_vec_to_string(vec);
  }

  template<>
  std::string lexical_cast<std::string>(const std::vector<uint32_t>& vec) {
    return graphlab_vec_to_string(vec);
  }

  template<>
  std::string lexical_cast<std::string>(const std::vector<uint64_t>& vec) {
    return graphlab_vec_to_string(vec);
  }

  template<>
  std::string lexical_cast< std::string >(const std::vector<double>& vec) {
    return graphlab_vec_to_string(vec);
  }

  template<>
  std::string lexical_cast< std::string>(const std::vector<float>& vec) {
    return graphlab_vec_to_string(vec);
  }

  template<>
  std::string lexical_cast< std::string>(const std::vector<std::string>& vec) {
    return graphlab_vec_to_string(vec);
  }
};


namespace graphlab {
 
static const char* engine_help_string = 
#include <graphlab/options/engine_help.txt>
;

static const char* graph_help_string = 
#include <graphlab/options/graph_help.txt>
;



  bool command_line_options::parse(int argc, const char* const* argv,
                                   bool allow_unregistered) {
    namespace boost_po = boost::program_options;
    
    size_t ncpus(get_ncpus());
    std::string engine_opts_string;
    std::string schedulertype(get_scheduler_type());
    std::string scheduler_opts_string = "";
    std::string graph_opts_string = "";

    if(!suppress_graphlab_options) {
      // Set the program options
      desc.add_options()
        ("ncpus",
        boost_po::value<size_t>(&(ncpus))->
        default_value(ncpus),
        "Number of cpus to use per machine. Defaults to (#cores - 2)")
        ("scheduler",
          boost_po::value<std::string>(&(schedulertype))->
          default_value(schedulertype),
          (std::string("Supported schedulers are: "
          + get_scheduler_names_str() +
          ". Too see options for each scheduler, run the program with the option"
          " ---schedhelp=[scheduler_name]").c_str()))
        ("engine_opts",
        boost_po::value<std::string>(&(engine_opts_string))->
        default_value(engine_opts_string),
        "string of engine options i.e., \"timeout=100\"")
        ("graph_opts",
          boost_po::value<std::string>(&(graph_opts_string))->
          default_value(graph_opts_string),
          "String of graph options i.e., \"ingress=random\"")
        ("scheduler_opts",
          boost_po::value<std::string>(&(scheduler_opts_string))->
          default_value(scheduler_opts_string),
          "String of scheduler options i.e., \"strict=true\"")
        ("engine_help",
          boost_po::value<std::string>()->implicit_value(""),
          "Display help for engine options.")
        ("graph_help",
        boost_po::value<std::string>()->implicit_value(""),
        "Display help for the distributed graph.")
        ("scheduler_help",
          boost_po::value<std::string>()->implicit_value(""),
          "Display help for schedulers.");
    }
    // Parse the arguments
    try {
      std::vector<std::string> arguments;
      std::copy(argv + 1, argv + argc + !argc, 
                std::inserter(arguments, arguments.end()));

      boost_po::command_line_parser parser(arguments);
      parser.options(desc);
      if (allow_unregistered) parser.allow_unregistered();
      if (num_positional) parser.positional(pos_opts);
      boost_po::parsed_options parsed = parser.run();
      if (allow_unregistered) {
        unrecognized_options = 
            boost_po::collect_unrecognized(parsed.options, 
                                           boost_po::include_positional);
      } else {
        unrecognized_options.clear();
      }
      boost_po::store(parsed, vm);
      boost_po::notify(vm);
    } catch( boost_po::error error) {
      std::cout << "Invalid syntax:\n"
                << "\t" << error.what()
                << "\n\n" << std::endl
                << "Description:"
                << std::endl;
      print_description();
      return false;
    }
    if(vm.count("help")) {
      print_description();
      return false;
    }
    if (vm.count("scheduler_help")) {
      std::string schedname = vm["scheduler_help"].as<std::string>();
      if (schedname != "") {
        print_scheduler_info(schedname, std::cout);
      } else {
        std::vector<std::string> schednames = get_scheduler_names();
        for(size_t i = 0;i < schednames.size(); ++i) {
          print_scheduler_info(schednames[i], std::cout);
        }
      }
      return false;
    }    
    if (vm.count("engine_help")) {
      std::cout << engine_help_string; 
      return false;
    }
    if (vm.count("graph_help")) {
      std::cout << graph_help_string; 
      return false;
    } 
    set_ncpus(ncpus);

    set_scheduler_type(schedulertype);

    get_scheduler_args().parse_string(scheduler_opts_string);
    get_engine_args().parse_string(engine_opts_string);
    get_graph_args().parse_string(graph_opts_string);
    return true;
  } // end of parse


  bool command_line_options::is_set(const std::string& option) {
    return vm.count(option);
  }

  void command_line_options::add_positional(const std::string& str) {
    num_positional++;
    pos_opts.add(str.c_str(), 1);
  }
}

