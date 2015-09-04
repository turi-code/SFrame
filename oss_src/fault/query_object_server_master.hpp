/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef QUERY_OBJECT_SERVER_MASTER_HPP
#define QUERY_OBJECT_SERVER_MASTER_HPP
#include <stdint.h>
#include <vector>
#include <string>
#include <iostream>
#include <boost/algorithm/string.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <zookeeper_util/key_value.hpp>
#include <fault/sockets/async_reply_socket.hpp>
#include <fault/sockets/publish_socket.hpp>
#include <fault/query_object_server_common.hpp>
#include <fault/query_object.hpp>
#include <fault/zmq/zmq_msg_vector.hpp>
namespace libfault {

/**
 * \ingroup fault
 */
struct query_object_server_master {

  std::string objectkey; // the object key associated with this object
  query_object* qobj; // The query object
  async_reply_socket* repsock; // the reply socket associated with the query object
  publish_socket* pubsock; // If this is a master, it also has an associated



  boost::shared_mutex query_obj_rwlock;

  socket_receive_pollset pollset;

  query_object_server_master(void* zmq_ctx,
                             graphlab::zookeeper_util::key_value* zk_keyval,
                             std::string objectkey,
                             query_object* qobj);

  ~query_object_server_master();

  bool master_reply_callback(zmq_msg_vector& recv,
                             zmq_msg_vector& reply);

  int start();
};



} // namespace libfault
#endif
