/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <shmipc/shmipc.hpp>
#include <string.h>
#include <iostream>
using namespace graphlab;
int main(int argc, char** argv) {
  // ./shm_ping_server_test --help
  // or too many arguments
  if ((argc == 2 && strcmp(argv[1], "--help") == 0) || argc > 2) {
    std::cout << "Usage: " << argv[0] << " [ipc file name]\n";
    return 1;
  }
  shmipc::server server;

  if (argc >= 2) {
    server.bind(argv[1]);
  } else {
    server.bind();
  }
  std::cout << server.get_shared_memory_name() << std::endl;
  while(1) {
    bool ok = server.wait_for_connect(1);
    if (ok) break;
    else {
      std::cout << "timeout" << std::endl;
    }
  }
  std::cout << "Connected" << std::endl;
  char *c = nullptr;
  size_t len = 0;  
  while(1) {
    size_t receivelen = 0;
    bool ok = server.receive_direct(&c, &len, receivelen, 10);
    if (receivelen >= 3) {
      if (strncmp(c, "end", 3) == 0) break;
    } 
    server.send(c, receivelen);
  }
}
