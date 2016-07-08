/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <iostream>
#include <string>
#include <sstream>
#include <thread>

int main(int argc, char **argv) {
  std::stringstream ss;
  ss << "Hello world! ";
  for(int i = 0; i < argc; ++i) {
    ss << argv[i];
    ss << " ";
  }
  
  std::cout << ss.str();

  //std::cerr << "ERRORS AND STUFF!!!";

  return 0;
}
