/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_LAMBDA_WORKER_POOL_HPP
#define GRAPHLAB_LAMBDA_WORKER_POOL_HPP

#include<lambda/worker_connection.hpp>
#include<parallel/lambda_omp.hpp>
#include<logger/assertions.hpp>
#include<boost/filesystem/operations.hpp>
#include<boost/filesystem/path.hpp>
#include <parallel/pthread_tools.hpp>
#include<fileio/fs_utils.hpp>
#include<fileio/temp_files.hpp>
#include<process/process.hpp>

namespace graphlab {

namespace lambda {

/**
 * Create a worker process using given binary path and the worker_address.
 *
 * Returns a pointer to the worker_connection or throws an exception if failed.
 */
template<typename ProxyType>
std::shared_ptr<worker_connection<ProxyType> > spawn_worker(std::vector<std::string> worker_binary_args,
                                                           std::string worker_address,
                                                           int connection_timeout) {

  ASSERT_MSG(worker_binary_args.size() >= 1, "Error in worker binary specification.");

  const std::string& worker_binary = worker_binary_args.front();
  
  namespace fs = boost::filesystem;
  
  auto the_path = fs::path(worker_binary);
  if (!fs::exists(the_path)) {
    throw std::string("lambda_worker executable: ") + worker_binary + " not found.";
  }

  logstream(LOG_INFO) << "Start lambda worker at " << worker_address
                      << " using binary: " << worker_binary << std::endl;
  auto worker_proc = std::make_shared<process>();

  std::vector<std::string> args(worker_binary_args.begin() + 1, worker_binary_args.end());
  args.push_back(worker_address);
  
  if(!worker_proc->launch(worker_binary, std::vector<std::string>{worker_address})) {
    throw("Fail launching lambda worker.");
  }

  worker_proc->autoreap();
  size_t retry = 0;
  size_t max_retry = 3;
  std::unique_ptr<cppipc::comm_client> cli;
  bool success = false;
  while (retry < max_retry) {
    // Make sure the server process is still around
    if (!worker_proc->exists()) {
      logstream(LOG_ERROR) << "Lambda worker process terminated unexpectedly" << std::endl;
      break;
    }

    // Try connecting to the server
    retry++;
    try {
      std::unique_ptr<cppipc::comm_client> tmp_cli(new cppipc::comm_client({}, worker_address, connection_timeout));
      cppipc::reply_status status = tmp_cli->start();
      if (status == cppipc::reply_status::OK) {
        // Success
        logstream(LOG_INFO) << "Connected to worker at " << worker_address << std::endl;
        cli.swap(tmp_cli);
        success = true;
        break;
      } else {
        // Connecting to server failed
        logstream(LOG_ERROR) << "Fail connecting to worker at " << worker_address
          << ". Status: " << cppipc::reply_status_to_string(status) 
          << ". Retry: " << retry << std::endl;
      }
      // Exception happended during comm_client construction/starting
    } catch (std::string error) {
      logstream(LOG_ERROR) << error << std::endl;
      break;
    } catch (...) {
      logstream(LOG_ERROR) << "Error starting cppipc client at " << worker_address << std::endl;
      break;
    }
  } // end of while

  if (!success) {
    throw std::string("Fail launching lambda worker.");
  } else {
    return std::make_shared<worker_connection<ProxyType>>(worker_proc, worker_address, cli);
  }
}

template<typename ProxyType>
std::shared_ptr<worker_connection<ProxyType> > spawn_worker(std::string worker_binary_args,
                                                           std::string worker_address,
                                                           int connection_timeout) {
  return spawn_worker<ProxyType>(std::vector<std::string>{worker_binary_args},
                                 worker_address, connection_timeout);
}

/**
 * Forward declaration. RAII for worker proxy allocation.
 */
template<typename ProxyType>
class worker_guard;

/**
 * Worker pool manages a pool of spawned worker connections.
 *
 * The worker pool is fix sized, and all the worker processes
 * are created at the creation of worker pool, and destroyed
 * together with the worker pool.
 */
template<typename ProxyType>
class worker_pool {

 public:
  typedef std::shared_ptr<worker_connection<ProxyType>> worker_conn_ptr;

 public:
  worker_pool(size_t nworkers,
              const std::string& worker_binary,
              std::vector<std::string> worker_addresses={},
              int connection_timeout=5)
      : worker_pool(nworkers, std::vector<std::string>{worker_binary},
                    worker_addresses, connection_timeout)
  {}
  
  /**
   * Creates a worker pool of given size.
   * The actual pool size may be smaller due to resource limitation.
   * Throws exception if zero worker is started.
   */
  worker_pool(size_t nworkers,
              const std::vector<std::string>& worker_binary_and_args,
              std::vector<std::string> worker_addresses={},
              int connection_timeout=5) {
    
    if (worker_addresses.empty()) {
      for (size_t i = 0; i < nworkers; ++i)
        worker_addresses.push_back("ipc://" + get_temp_name());
    }
    ASSERT_EQ(worker_addresses.size(), nworkers);
    ASSERT_GT(nworkers, 0);

    m_worker_binary_and_args = worker_binary_and_args;
    m_connection_timeout = connection_timeout; 

    parallel_for(0, nworkers, [&](size_t i) {
      try {
        auto worker_conn = spawn_worker<ProxyType>(
            worker_binary_and_args, worker_addresses[i], m_connection_timeout);

        std::unique_lock<graphlab::mutex> lck(mtx);
        connections.push_back(worker_conn);
        available_workers.push_back(worker_conn->get_proxy());
        proxy_to_connection[worker_conn->get_proxy()] = connections.size()-1;
        m_worker_addresses.push_back(worker_addresses[i]);
      } catch(std::string e) {
        logstream(LOG_ERROR) << e << std::endl;
      } catch(const char* e) {
        logstream(LOG_ERROR) << e << std::endl;
      } catch (...) {
        std::exception_ptr eptr = std::current_exception();
        logstream(LOG_ERROR) << "Fail spawning worker " << i << ". Unknown failure" << std::endl;
        try {
          if(eptr) {
            std::rethrow_exception(eptr);
          }
        } catch (const std::exception& e) {
          logstream(LOG_ERROR) << "Caught exception \"" << e.what() << "\"\n";
        }
        // spawning worker might fail. The acutal pool size may have
        // less worker or no worker at all.
      }
    });

    if (num_workers() == 0) {
      log_and_throw("Unable to evaluate lambdas. Lambda workers did not start.");
    } else if (num_workers() < nworkers) {
      logprogress_stream << "Less than " << nworkers << " successfully started. "
                         << "Using only " << num_workers()  << " workers." << std::endl;
      logprogress_stream << "All operations will proceed as normal, but "
                         << "lambda operations will not be able to use all "
                         << "available cores." << std::endl;
      logprogress_stream << "To help us diagnose this issue, please send "
                         << "the log file to product-feedback@dato.com." << std::endl;
      logprogress_stream << "(The location of the log file is printed at the "
                         << "start of the GraphLab server)."  << std::endl;
      logstream(LOG_ERROR) << "Less than " << nworkers << " successfully started."
                           << "Using only " << num_workers() << std::endl;
    }
  }

  ~worker_pool() {
    available_workers.clear();
    proxy_to_connection.clear();
    for (auto& conn : connections) {
      deleted_connections.push_back(std::move(conn));
    }
    parallel_for(0, deleted_connections.size(), [&](size_t i) {
      deleted_connections[i].reset();
    });
  }

  /**
   * Returns a worker proxy that is available, block till there is worker available.
   * Thread-safe.
   */
  std::shared_ptr<ProxyType> get_worker() {
    std::unique_lock<graphlab::mutex> lck(mtx);
    while (available_workers.empty()) {
      cv.wait(lck);
    }
    auto proxy = available_workers.front();
    available_workers.pop_front();
    return proxy;
  }

  /**
   * Put a worker proxy back to avaiable queue. Has no effect if the worker proxy
   * is already available.
   * Thread-safe.
   */
  void release_worker(std::shared_ptr<ProxyType> worker_proxy) {
    std::unique_lock<graphlab::mutex> lck(mtx);
    for (auto& p : available_workers) {
      if (p == worker_proxy)
        return;
    }
    available_workers.push_back(worker_proxy);
    lck.unlock();
    cv.notify_one();
  }

  /**
   * Returns a worker_guard for the given worker proxy.
   * When the worker_guard goes out of the scope, the guarded
   * worker_proxy will be automatically released.
   */
  std::shared_ptr<worker_guard<ProxyType>> get_worker_guard(std::shared_ptr<ProxyType> worker_proxy) {
    return std::make_shared<worker_guard<ProxyType>>(*this, worker_proxy);
  }

  /**
   * Call the function on all worker in parallel and return the results.
   */
  template<typename RetType, typename Fn>
  std::vector<RetType> call_all_workers(Fn f) {
    std::unique_lock<graphlab::mutex> lck(mtx);
    /// Block until all workers are avaiable, and lock all of them.
    while (available_workers.size() != connections.size()) {
      cv.wait(lck);
    }
    std::vector<RetType> ret(num_workers());
    parallel_for (0, num_workers(), [&](size_t i) {
      auto worker_proxy = available_workers[i];
      ret[i] = f(worker_proxy);
    });
    return ret;
  }

  /**
   * Returns number of workers in the pool.
   */
  size_t num_workers() const {
    return connections.size();
  }

  /**
   * Returns number of available workers in the pool. 
   */
  size_t num_available_workers() {
    std::unique_lock<graphlab::mutex> lck(mtx);
    return available_workers.size();
  }

  /**
   * Return the process object associated with a worker proxy.
   * Return -1 if the worker proxy not exist.
   */
  std::shared_ptr<process> get_process(std::shared_ptr<ProxyType> worker_proxy) {
    std::unique_lock<graphlab::mutex> lck(mtx);
    if (proxy_to_connection.count(worker_proxy)) {
      auto conn = connections[proxy_to_connection[worker_proxy]];
      return conn->get_process();
    } else {
      return NULL;
    }
  }

  /**
   * Restart the worker
   */
  void restart_worker(std::shared_ptr<ProxyType> worker_proxy) {
    std::unique_lock<graphlab::mutex> lck(mtx);
    DASSERT_TRUE(proxy_to_connection.count(worker_proxy) == 1);
    size_t id = proxy_to_connection[worker_proxy];
    logstream(LOG_INFO) << "Restart lambda worker " << worker_proxy << std::endl;
    auto& conn = connections[id];
    auto address = conn->get_address();
    logstream(LOG_INFO) << "Old worker pid: " << conn->get_pid()
                        << " address: " << address;
    // remove the old ipc socket file if it still exists
    size_t pos = address.find("ipc://");
    if (pos != std::string::npos) {
      fileio::delete_path(address.substr(6));
    }

    // Move the dead connection to deleted_connection
    // the reason we cannot destroy the old connection object directly 
    // is because the connection objects holds the unique ownership of
    // the comm_client object, however, at this point there might still
    // be shared pointers of the proxy object flying around and the client
    // object must exist for the proxy object to be properly cleaned up.
    deleted_connections.push_back(std::move(conn));

    // spawn new worker
    try {
      conn = (spawn_worker<ProxyType>(m_worker_binary_and_args, address, m_connection_timeout));
      logstream(LOG_INFO) << "New worker pid: " << conn->get_pid()
                          << " address: " << address << std::endl;

      logstream(LOG_INFO) << "Successfully restarted lambda worker. New proxy: "
                          << conn->get_proxy() << std::endl;

      // update bookkeepers
      proxy_to_connection.erase(proxy_to_connection.find(worker_proxy));
      proxy_to_connection[conn->get_proxy()] = id;
      auto iter = std::find(available_workers.begin(), available_workers.end(), worker_proxy);
      if (iter != available_workers.end()) {
        available_workers.erase(iter);
      }
      available_workers.push_back(conn->get_proxy());
      lck.unlock();
      cv.notify_one();
    } catch (std::exception e){
      logstream(LOG_INFO) << "Fail restarting lambda worker. " << e.what() << std::endl;
    } catch (std::string e){
      logstream(LOG_INFO) << "Fail restarting lambda worker. " << e << std::endl;
    } catch (const char* e) {
      logstream(LOG_INFO) << "Fail restarting lambda worker. " << e << std::endl;
    } catch (...) {
      logstream(LOG_INFO) << "Fail restarting lambda worker. Unknown error." << std::endl;
    }
  }

 private:
  std::vector<worker_conn_ptr> connections;

  std::vector<worker_conn_ptr> deleted_connections;

  std::map<std::shared_ptr<ProxyType>, size_t> proxy_to_connection;

  std::deque<std::shared_ptr<ProxyType>> available_workers;

  std::vector<std::string> m_worker_binary_and_args;
  std::vector<std::string> m_worker_addresses;
  int m_connection_timeout;

  graphlab::mutex mtx;
  graphlab::condition_variable cv;
}; // end of worker_pool


/**
 * RAII for allocating worker proxy.
 * When the worker_guard is destoryed, the guarded worker
 * be released by the worker_pool.
 */
template<typename ProxyType>
class worker_guard {
 public:
   worker_guard(worker_pool<ProxyType>& worker_pool_,
                std::shared_ptr<ProxyType> worker_proxy_)
     : m_pool(worker_pool_), m_proxy(worker_proxy_) { }

   ~worker_guard() {

     auto proc = m_pool.get_process(m_proxy);
     if (proc && proc->exists()) {
       // Child still alive
       m_pool.release_worker(m_proxy);
     } else {
       logstream(LOG_ERROR) << "Process of worker " << m_proxy << " has crashed" << std::endl;
       // restart the worker process
       m_pool.restart_worker(m_proxy);
     }
   }

 private:
   worker_pool<ProxyType>& m_pool;
   std::shared_ptr<ProxyType> m_proxy;
};

} // end of lambda namespace
} // end of graphlab namespace
#endif
