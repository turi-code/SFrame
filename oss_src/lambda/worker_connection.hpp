/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_LAMBDA_WORKER_CONNECTION_HPP
#define GRAPHLAB_LAMBDA_WORKER_CONNECTION_HPP

#include <parallel/mutex.hpp>
#include <unistd.h>
#include<cppipc/client/comm_client.hpp>
#include <process/process.hpp>

namespace graphlab {

namespace lambda {

/**
 * Manages a connection to a spawned lambda worker.
 *
 * When the connection object is destroyed, the corresponding
 * worker process will be killed.
 */
template<typename ProxyType>
class worker_connection {
 public:
  worker_connection(std::shared_ptr<process> proc,
                    std::string address,
                    std::unique_ptr<cppipc::comm_client>& client)
    : m_proc(proc), m_address(address), m_client(std::move(client)) {
      m_proxy.reset(new ProxyType(*m_client));
    }

  ~worker_connection() noexcept {
    try {
      m_client->stop();
    } catch (...) {
      logstream(LOG_ERROR) << "Fail stopping worker connection to process pid: "
                           << m_proc->get_pid() << std::endl;
    }
    if (!m_proxy.unique()) {
      logstream(LOG_ERROR) << "Worker proxy " << m_proxy
                           << " not unique" << std::endl;
    }
    m_proxy.reset();
    m_proc->kill(false);
  }

  std::shared_ptr<ProxyType> get_proxy() {
    return m_proxy;
  }

  size_t get_pid() {
    return m_proc->get_pid();
  }

  std::shared_ptr<process> get_process() {
    return m_proc;
  }

  std::string get_address() {
    return m_address;
  }

 private:
  std::shared_ptr<process> m_proc;
  std::string m_address;
  std::unique_ptr<cppipc::comm_client> m_client;
  std::shared_ptr<ProxyType> m_proxy;
};


} // end of lambda namespace
} // end of graphlab namespace
#endif
