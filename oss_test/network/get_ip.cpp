/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <iostream>
#include <network/net_util.hpp>

using namespace graphlab;

int main(int argc, char **argv) {
  std::cerr << get_local_ip_as_str(true);
}
