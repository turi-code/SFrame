/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <thread>
#include <iostream>

int main(int argc, char **argv) {
  std::cout << "Sleeping!" << std::endl;
  std::this_thread::sleep_for(std::chrono::seconds(10));
  return 0;
}

