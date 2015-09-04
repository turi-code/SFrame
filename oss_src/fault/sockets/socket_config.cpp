/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <cassert>
#include <sstream>
#include <zmq.h>
#include <boost/algorithm/string.hpp>
#include <boost/integer_traits.hpp>
#include <util/md5.hpp>

namespace libfault {
int SEND_TIMEOUT = 3000; 
int RECV_TIMEOUT = 7000;

void set_send_timeout(int ms) {
  SEND_TIMEOUT = ms;
}
void set_recv_timeout(int ms) {
  RECV_TIMEOUT = ms;
}


void set_conservative_socket_parameters(void* z_socket) {
  int lingerval = 500;
  int timeoutms = 500;
  int hwm = 0;
  int rc = zmq_setsockopt(z_socket, ZMQ_LINGER, &lingerval, sizeof(lingerval));
  assert(rc == 0);
  rc = zmq_setsockopt(z_socket, ZMQ_RCVTIMEO, &timeoutms, sizeof(timeoutms));
  assert(rc == 0);
  rc = zmq_setsockopt(z_socket, ZMQ_SNDTIMEO, &timeoutms, sizeof(timeoutms));
  assert(rc == 0);
  
  rc = zmq_setsockopt(z_socket, ZMQ_SNDHWM, &hwm, sizeof(hwm));
  assert(rc == 0);
  rc = zmq_setsockopt(z_socket, ZMQ_RCVHWM, &hwm, sizeof(hwm));
  assert(rc == 0);
  
}

/**
 * Given a string, returns a zeromq localhost tcp address (ex:
 * tcp://127.15.21.22:11111).  
 *
 * On windows we don't get zeromq IPC sockets. Hence, the easiest thing to do
 * is to remap ipc sockets to tcp addresses.
 *
 * When the server is started with address=default mode, on *nix systems we
 * map to ipc://something or order. Instead, on windows systems
 * we must map to tcp://[arbitrary local IP] where there is optimally, 
 * a 1-1 correspondence between the local IP and the PID.
 * 
 * So, here are the rules:
 * - We cannot generate any port number <= 1024
 * - Lets about 127.0.0.1 because too many stuff like to live there
 * - 127.0.0.0 is invalid. (network address) 
 * - 127.255.255.255 is invalid (broadcast address)
 */
std::string hash_string_to_tcp_address(const std::string& s) {
  std::string md5sum = graphlab::md5_raw(s);
  // we get approximately 5 bytes of entropy (yes really somewhat less) 

  unsigned char addr[4];
  addr[0] = 127;
  addr[1] = md5sum[0];
  addr[2] = md5sum[1];
  addr[3] = md5sum[2];
  uint16_t port = (uint16_t)(md5sum[3]) * 256 + md5sum[4];

  // validate
  if ((addr[1] == 0 && addr[2] == 0 && addr[3] == 0) ||  // network address
      (addr[1] == 0 && addr[2] == 0 && addr[3] == 1) ||  // common address
      (addr[1] == 255 && addr[2] == 255 && addr[3] == 255) ||  // broadcast
      (port <= 1024)) { // bad port
    // bad. this is network name
    // rehash
    return hash_string_to_tcp_address(md5sum);
  }

  // ok generate the string
  std::stringstream strm; 
  strm << "tcp://" << (int)(addr[0]) << "."
                   << (int)(addr[1]) << "."
                   << (int)(addr[2]) << "."
                   << (int)(addr[3]) << ":"
                   << (int)(port);
  return strm.str();
}

std::string normalize_address(const std::string& address) {
#ifdef _WIN32
  if (boost::starts_with(address, "ipc://")) {
    return hash_string_to_tcp_address(address);
  } else {
    return address;
  }
#endif
  return address;
}
};
