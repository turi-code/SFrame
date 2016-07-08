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
#include <zookeeper_util/server_list.hpp>

void callback(graphlab::zookeeper_util::server_list* caller, 
              std::string name_space,
              std::vector<std::string> server) {
  std::cout << "Watch triggered on " << name_space << "\n";
  for (size_t i = 0;i < server.size(); ++i) {
    std::cout << "\t" << server[i] << "\n";
  }
}

int main(int argc, char** argv) {
  if (argc != 4) {
    std::cout << "Usage: zookeeper_test [zkhost] [prefix] [name]\n";
    return 0;
  }
  std::string zkhost = argv[1];
  std::string prefix = argv[2];
  std::string name = argv[3];
  std::vector<std::string> zkhosts; zkhosts.push_back(zkhost);
  graphlab::zookeeper_util::server_list server_list(zkhosts, prefix, name);

  std::cout << "Commands: \n";
  std::cout << "Join: j [namespace]\n";
  std::cout << "Leave: l [namespace]\n";
  std::cout << "Query: q [namespace]\n";
  std::cout << "Watch: w [namespace]\n";
  std::cout << "Stop Watch: u [namespace]\n";
  std::cout << "Stop: s\n";
  std::cout << "\n\n";


  server_list.set_callback(callback);

  while(1) {
    char command;
    std::string name;

    std::cin >> command;
    if (command == 's') break;
    else if (command == 'j' || 
             command == 'l' || 
             command == 'q' || 
             command == 'w' || 
             command == 'u') std::cin >> name;
    
    if (command == 'j') {
      server_list.join(name);
    } 
    else if (command == 'l') {
      server_list.leave(name);
    }
    else if (command == 'w') {
      std::vector<std::string> ret = server_list.watch_changes(name);
      for (size_t i = 0;i < ret.size(); ++i) {
        std::cout << "\t" << ret[i] << "\n";
      }
    }
    else if (command == 'u') {
      server_list.stop_watching(name);
    }
    else if (command == 'q') {
      std::vector<std::string> ret = server_list.get_all_servers(name);
      for (size_t i = 0;i < ret.size(); ++i) {
        std::cout << "\t" << ret[i] << "\n";
      }
    }
  }
}
