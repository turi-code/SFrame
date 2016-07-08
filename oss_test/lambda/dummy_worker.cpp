/*
* Copyright (C) 2016 Turi
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
#include <cppipc/cppipc.hpp>
#include <process/process_util.hpp>
#include "dummy_worker_interface.hpp"
#include <fault/sockets/socket_config.hpp>
#include <thread>

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
  if (argc != 2) {
    std::cerr << "Usage: ./dummy_worker ipc:///tmp/test_address" << std::endl;
    exit(1);
  }

  // Manually set this one.
  char* use_fallback = std::getenv("GRAPHLAB_FORCE_IPC_TO_TCP_FALLBACK");
  if(use_fallback != nullptr && std::string(use_fallback) == "1") {
    libfault::FORCE_IPC_TO_TCP_FALLBACK = true;
  }

  size_t parent_pid = get_parent_pid();
  // Options
  std::string program_name = argv[0];
  std::string server_address = argv[1];

  // construct the server
  cppipc::comm_server server(std::vector<std::string>(), "", server_address);
  server.register_type<dummy_worker_interface>([](){ return new dummy_worker_obj(); });

  server.start();

  //Wait for parent that spawned me to die
  //TODO: I don't think we need to write any explicit code for this on Unix,
  //but we may on Windows. Revisit later.
  wait_for_parent_exit(parent_pid);
}
