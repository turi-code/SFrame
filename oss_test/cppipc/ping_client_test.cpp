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
#include <cppipc/client/comm_client.hpp>
#include <iostream>
int main(int argc, char** argv) {
  //cppipc::comm_client client({"localhost:2181"}, "pingtest");
  cppipc::comm_client client({}, "tcp://127.0.0.1:19000");
  //cppipc::comm_client client({}, "ipc:///tmp/ping_server_test");
  client.start();
  std::cout << "Ping test. \"quit\" to quit\n";
  while(1) {
    std::string s;
    std::cin >> s;
    try {
      std::string ret = client.ping(s);
      std::cout << "pong: " << ret << "\n";
    } catch (cppipc::reply_status status) {
      std::cout << "Exception: " << cppipc::reply_status_to_string(status) << "\n";
    }
    if (s == "quit") break;
  }
}
