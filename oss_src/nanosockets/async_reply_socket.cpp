/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <cstdlib>
#include <cassert>
#include <cstdlib>
#include <iostream>
#include <mutex>
#include <parallel/atomic.hpp>
#include <boost/bind.hpp>
#include <logger/logger.hpp>
#include <nanosockets/socket_errors.hpp>
#include <nanosockets/socket_config.hpp>
#include <nanosockets/async_reply_socket.hpp>
#include <nanosockets/print_zmq_error.hpp>
#include <network/net_util.hpp>
extern "C" {
#include <nanomsg/nn.h>
#include <nanomsg/reqrep.h>
}
namespace graphlab {
namespace nanosockets {


async_reply_socket::async_reply_socket(callback_type callback,
                           size_t nthreads,
                           std::string bind_address) {
  this->callback = callback;
  z_socket = nn_socket(AF_SP_RAW, NN_REP);
  set_conservative_socket_parameters(z_socket);
  local_address = normalize_address(bind_address);
  int rc = nn_bind(z_socket, local_address.c_str());
  if (rc == -1) {
    print_zmq_error("async_reply_socket construction: ");
    assert(rc == 0);
  }

  for (size_t i = 0;i < nthreads; ++i) {
    threads.launch(std::bind(&async_reply_socket::thread_function,
                             this));
                             
  }
//   threads.launch(std::bind(&async_reply_socket::poll_function,
//                              this));
}

void async_reply_socket::close() {
  if (z_socket != -1) {
    // kill all threads
    queuelock.lock();
    queue_terminate = true;
    queuecond.notify_all();
    queuelock.unlock();
    threads.join();
    nn_close(z_socket);
    z_socket = -1;
  }
}

async_reply_socket::~async_reply_socket() {
  close();
}



void async_reply_socket::poll_function() {

  while(1) {
    struct nn_pollfd pfd[1];
    pfd[0].fd = z_socket;
    pfd[0].events = NN_POLLIN;
    pfd[0].revents = 0;
    nn_poll (pfd, 1, 1000);
    if (queue_terminate) return;
    if ((pfd[0].revents & NN_POLLIN) == 0) {
        continue;
    }
    job j;
    // receive a message
    struct nn_iovec iov;
    struct nn_msghdr hdr;

    iov.iov_base = reinterpret_cast<void*>(&(j.data));
    iov.iov_len = NN_MSG;
    hdr.msg_iov = &iov;
    hdr.msg_iovlen = 1;
    hdr.msg_control = reinterpret_cast<void*>(&(j.control));
    
    std::unique_lock<mutex> socklock(socketlock);
    int rc = nn_recvmsg(z_socket, &hdr, 0);
    if (rc == -1) {
      print_zmq_error("async_reply_socket poll: ");
      continue;
    }
    j.datalen = rc;
    socketlock.unlock();

    std::unique_lock<mutex> lock(queuelock);
    jobqueue.push(j);
    queuecond.signal();
    lock.unlock();
  }
}


void async_reply_socket::process_job(job j) {
  zmq_msg_vector query, reply;
  // deserialize query and perform the call
  iarchive iarc(j.data, j.datalen);
  iarc >> query;
  callback(query, reply);

  // release some memory
  query.clear();

  // serialize the reply
  oarchive oarc;
  oarc << reply;

  struct nn_iovec iov;
  struct nn_msghdr hdr;
  iov.iov_base = oarc.buf;
  iov.iov_len = oarc.off;
  hdr.msg_iov = &iov;
  hdr.msg_iovlen = 1;
  hdr.msg_control = reinterpret_cast<void*>(&(j.control));
  hdr.msg_controllen = NN_MSG;
  
  std::lock_guard<mutex> socklock(socketlock);
  int rc = nn_sendmsg(z_socket, &hdr, 0);
  free(oarc.buf);
  nn_freemsg(j.data);
  nn_freemsg(j.control);
  if (rc == -1) print_zmq_error("send failure : ");
}
// 
// void async_reply_socket::thread_function() {
//   std::unique_lock<mutex> lock(queuelock);
//   while(1) {
//     while(jobqueue.empty() && !queue_terminate) {
//       queuecond.wait(lock);
//     }
//     // at this point, we have the lock, either
//     // jobqueue has something, or queue_terminate is set
//     if (queue_terminate) break;
//     assert(!jobqueue.empty());
//     job j = jobqueue.front();
//     jobqueue.pop();
//     // we process the job outside of the lock
//     lock.unlock();
// 
//     // this function also frees msg
//     process_job(j);
// 
//     lock.lock();
//   }
// }
// 

void async_reply_socket::thread_function() {
  while(!queue_terminate) {
    // jobqueue has something, or queue_terminate is set
    if (queue_terminate) break;
    char* body;
    void* control;
    struct nn_iovec iov;
    struct nn_msghdr hdr;

    memset (&hdr, 0, sizeof (hdr));
    control = NULL;
    iov.iov_base = &body;
    iov.iov_len = NN_MSG;
    hdr.msg_iov = &iov;
    hdr.msg_iovlen = 1;
    hdr.msg_control = reinterpret_cast<void*>(&control);
    hdr.msg_controllen = NN_MSG;

    int rc = nn_recvmsg (z_socket, &hdr, 0);
    if (rc < 0) {
      if (nn_errno() == EAGAIN || nn_errno() == EINTR || nn_errno() == ETIMEDOUT) {
        continue;
      } else {
        print_zmq_error("Unexpected error in recvmsg:");
        break;
      }
    }

    zmq_msg_vector query, reply;
    // deserialize query and perform the call
    iarchive iarc(body, rc);
    iarc >> query;
    bool hasreply = callback(query, reply);
    // release some memory
    nn_freemsg(body);
    query.clear();

    // serialize the reply
    oarchive oarc;
    oarc << reply;

    iov.iov_base = oarc.buf;
    iov.iov_len = oarc.off;
    hdr.msg_iovlen = 1;
    rc = nn_sendmsg (z_socket, &hdr, 0);
    if (rc < 0) {
      print_zmq_error("Unexpected error in sendmsg:");
      break;
    }

  }
}

std::string async_reply_socket::get_bound_address() {
  return local_address;
}

} // namespace nanosockets
}
