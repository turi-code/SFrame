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
#include <cppipc/server/comm_server.hpp>
#include <unistd.h>

int main(int argc, char** argv) {
  //cppipc::comm_server server({"localhost:2181"}, "pingtest");
  cppipc::comm_server server({}, "", "tcp://127.0.0.1:19000");
  //cppipc::comm_server server({}, "ipc:///tmp/ping_server_test");
  server.start();
  getchar();
  server.stop();
}
