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
#include <iostream>
#include <fault/query_object_client.hpp>

using namespace libfault;

int main(int argc, char** argv) {
  if (argc != 3) {
    std::cout << "Usage: echo_qo_test_client[zkhost] [prefix] \n";
    return 0;
  }
  std::string zkhost = argv[1];
  std::string prefix = argv[2];
  std::vector<std::string> zkhosts; zkhosts.push_back(zkhost);

  void* zmq_ctx = zmq_ctx_new();
  query_object_client client(zmq_ctx, zkhosts, prefix);
  std::cout << "[echotarget] [stuff]\n";
  std::cout << "An echotarget of \"q\" quits\n";

  std::cout << "\n\n\n";
  while(1) {
    std::string target;
    std::cin >> target;
    if (target == "q") break;
    std::string val;
    std::getline(std::cin, val);

    char* msg = (char*)malloc(val.length());
    memcpy(msg, val.c_str(), val.length());
    query_object_client::query_result result =
        client.update(target, msg, val.length());
    if (result.get_status() != 0) {
      std::cout << "\tError\n\n";
    } else {
      std::cout << "\tReply: " << result.get_reply() << "\n\n";
    }
    // this takes over the pointer
  }
}

