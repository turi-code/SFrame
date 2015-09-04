/*
* Copyright (C) 2015 Dato, Inc.
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Affero General Public License as
* published by the Free Software Foundation, either version 3 of the
* License, or (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU Affero General Public License for more details.
*
* You should have received a copy of the GNU Affero General Public License
* along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/
#include <boost/program_options.hpp>
#include <cppipc/cppipc.hpp>
#include <process/process_util.hpp>
#include "dummy_worker_interface.hpp"
#include <thread>

namespace po = boost::program_options;

using namespace graphlab;

class dummy_worker_obj : public dummy_worker_interface {
  public:
    std::string echo(const std::string& s) {
      return s;
    }
    void throw_error() {
      throw "error";
    }
    void quit(int exitcode=0) {
      exit(exitcode);
    }
};

int main(int argc, char** argv) {
  size_t parent_pid = get_parent_pid();
  // Options
  std::string program_name = argv[0];
  std::string server_address;

  // Handle server commandline options
  boost::program_options::variables_map vm;
  po::options_description desc("Allowed options");
  desc.add_options()
      ("help", "Print this help message.")
      ("server_address", 
       po::value<std::string>(&server_address)->implicit_value(server_address),
       "This must be a valid ZeroMQ endpoint and "
                         "is the address the server listens on");

  po::positional_options_description positional;
  positional.add("server_address", 1);

  // try to parse the command line options
  try {
    po::command_line_parser parser(argc, argv);
    parser.options(desc);
    parser.positional(positional);
    po::parsed_options parsed = parser.run();
    po::store(parsed, vm);
    po::notify(vm);
  } catch(std::exception& error) {
    std::cout << "Invalid syntax:\n"
              << "\t" << error.what()
              << "\n\n" << std::endl
              << "Description:"
              << std::endl;
    return 1;
  }

  // construct the server
  cppipc::comm_server server(std::vector<std::string>(), "", server_address);
  server.register_type<dummy_worker_interface>([](){ return new dummy_worker_obj(); });

  server.start();

  //Wait for parent that spawned me to die
  //TODO: I don't think we need to write any explicit code for this on Unix,
  //but we may on Windows. Revisit later.
  wait_for_parent_exit(parent_pid);
}
