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
  if ((argc == 2 && strcmp(argv[1], "--help") == 0) || argc != 2) {
    std::cout << argv[0] << " [ipc file name]\n";
    return 1;
  }
  shmipc::client client;
  client.connect(argv[1]);
  std::cout << "\"end\" to quit\n";
  size_t ctr = 0;
  while(1) {
    std::string s;
    std::cin >> s;
    client.send(s.c_str(), s.length());
    if (s == "end") break;
    char *c = nullptr;
    size_t len = 0;  
    size_t receivelen = 0;
    bool ok = client.receive_direct(&c, &len, receivelen, 10);
    std::cout << "Received:" << std::string(c, receivelen) << std::endl;
    ctr+=ok;
  }
}
