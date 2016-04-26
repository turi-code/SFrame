/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_LAMBDA_WORKER_POOL_HPP
#define GRAPHLAB_LAMBDA_WORKER_POOL_HPP

#include<lambda/lambda_utils.hpp>
#include<logger/assertions.hpp>
#include<fileio/temp_files.hpp>
#include<boost/filesystem/operations.hpp>
#include<boost/filesystem/path.hpp>
#include<parallel/lambda_omp.hpp>
#include<parallel/pthread_tools.hpp>
#include<process/process.hpp>
#include<cppipc/client/comm_client.hpp>

namespace graphlab {
namespace lambda {

/**
 * This class manages the all the resources of a single lambda worker.
 */
template<typename ProxyType>
struct worker_process {
  // worker_id. starts from 0 and increments by one
  size_t id; 
  // cppipc proxy object
  std::unique_ptr<ProxyType> proxy;
  // cppipc comm_client
  std::unique_ptr<cppipc::comm_client> client;
  // cppipc address
  std::string address;
  // process object
  std::unique_ptr<process> process_;

  // next avaiable worker id 
  static int get_next_id() {
    static atomic<int> next_id;
    next_id++;
    return next_id;
  }

  // constructor
  worker_process() {
    id = get_next_id();
  }

  // destructor
  ~worker_process() noexcept {
    logstream(LOG_INFO) << "Destroying worker_process " << id << std::endl;
    try {
      proxy.reset();
      client->stop();
      client.reset();
      process_->kill(false);
      process_.reset();
    } catch (...) {
      logstream(LOG_ERROR) << "Exception in destroying worker_process " << id << std::endl;
    }
  }
}; // end of worker_process

/**
 * Create a worker process using given binary path and the worker_address.
 * May throw exception on error.
 */
template<typename ProxyType>
std::unique_ptr<worker_process<ProxyType>> spawn_worker(std::vector<std::string> worker_binary_args,
                                                        std::string worker_address,
                                                        int connection_timeout) {
  namespace fs = boost::filesystem;

  // Sanity check arguments
  ASSERT_MSG(worker_binary_args.size() >= 1, "Unexpected number of arguments.");
  const std::string& worker_binary = worker_binary_args.front();
  auto the_path = fs::path(worker_binary);
  if (!fs::exists(the_path)) { throw std::string("Executable: ") + worker_binary + " not found."; }

  // Step 1: start a new process
  logstream(LOG_INFO) << "Start lambda worker at " << worker_address
                      << " using binary: " << worker_binary << std::endl;
  std::unique_ptr<process> new_process(new process());
  std::vector<std::string> args(worker_binary_args.begin() + 1, worker_binary_args.end());
  args.push_back(worker_address);
  if(new_process->launch(worker_binary, args) == false) {
    throw("Fail launching lambda worker.");
  }

  // Step 2: create cppipc client and connect it to the launched process 
  size_t retry = 0;
  size_t max_retry = 3;
  std::unique_ptr<cppipc::comm_client> new_client;
  while (retry < max_retry) {
    // Make sure the server process is still around
    if (!new_process->exists()) {
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
        new_client.swap(tmp_cli);
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
  if (new_client == nullptr) {
    throw std::string("Fail launching lambda worker. Reached maximum retry.");
  }

  // Step 3. Create proxy object
  std::unique_ptr<ProxyType> new_proxy(new ProxyType(*new_client));

  // Step 4. Return the worker process
  std::unique_ptr<worker_process<ProxyType>> ret(new worker_process<ProxyType>());
  ret->proxy.swap(new_proxy);
  ret->client.swap(new_client);
  ret->address = worker_address;
  ret->process_.swap(new_process);

  // Done!
  logstream(LOG_INFO) << "Successfully launched lambda worker " << ret->id 
                      << std::endl;
  return ret; 
} // end spawn worker


/**
 * Exception free wrapper of spawn_worker().
 * Return nullptr on exception, and log the error.
 */
template<typename ProxyType>
std::unique_ptr<worker_process<ProxyType>> try_spawn_worker(std::vector<std::string> worker_binary_args,
                                                            std::string worker_address,
                                                            int connection_timeout) noexcept {
  try {
    return spawn_worker<ProxyType>(worker_binary_args, worker_address, connection_timeout);
  } catch(std::string e) {
    logstream(LOG_ERROR) << e << std::endl;
  } catch(const char* e) {
    logstream(LOG_ERROR) << e << std::endl;
  } catch (...) {
    std::exception_ptr eptr = std::current_exception();
    logstream(LOG_ERROR) << "Fail spawning worker. Unknown error." << std::endl;
    try {
      if(eptr) {
        std::rethrow_exception(eptr);
      }
    } catch (const std::exception& e) {
      logstream(LOG_ERROR) << "Caught exception \"" << e.what() << "\"\n";
    }
  }
  return nullptr;
}

// Forward declaration
template<typename ProxyType>
class worker_guard;

                                                                                   
                                                                                   
/**
 * Manage a list of worker_processes.
 *
 * - Initialize:
 * The pool is initialized with a fixed number of workers. Due to system resource
 * limitation, the actual pool may contain less workers than intended.
 *
 * - Acquire worker:
 * User request a worker process by calling get_worker(),
 * which returns a unique_ptr and transfers the ownership of the worker process.
 *
 * - Release worker:
 * After the use of the worker, The requested worker_process must be returned
 * to the worker_pool by calling release_worker().
 * Alternatively, use the RAII pattern by calling get_worker_guard() to
 * guarantee return of the resource.
 *
 * get_worker()/release_worker() are thread safe.
 *
 * - Worker crash recovery:
 * On release_worker(), if the worker_process is dead, a new worker_process 
 * will be started and released back to the pool. In the worst case where
 * no new process can be started, the pool size will be decreased. 
 *
 * - Call all workers:
 * You can issue a function call to all workers using the call_all_workers()
 * function. It will block until all workers become avaialble, call the function
 * on all workers, and block until all the call finishes or exception is thrown. 
 * During the call if the worker_process is dead, replace the dead worker with
 * new process.
 *
 * - Hang 
 * It is extremely important to guarantee the release of a worker after use,
 * or the pool will hang on termination or barrier function such as
 * call_all_workers().
 */
template<typename ProxyType>
class worker_pool {
public:
  /**
   * Return the next available worker.
   * Block until any worker is available.
   * Throws error if worker_pool has zero workers. 
   *
   * \note: This function must be used in pair with release_worker(), or
   * get the RAII protector using get_worker_guard() when release_worker()
   * cannot be called reliably. Otherwise, the worker_pool may hang
   * waiting for worker to return.
   */
  std::unique_ptr<worker_process<ProxyType>> get_worker() {
    std::unique_lock<graphlab::mutex> lck(m_mutex);
    wait_for_one(lck);
    auto worker = std::move(m_available_workers.front());
    m_available_workers.pop_front();
    return worker;
  }

  /**
   * Returns a worker_guard for the given worker.
   * When the worker_guard goes out of the scope, the guarded
   * worker will be automatically released.
   */
  std::shared_ptr<worker_guard<ProxyType>> get_worker_guard(std::unique_ptr<worker_process<ProxyType>>& worker) {
    return std::make_shared<worker_guard<ProxyType>>(this, worker);
  }

  /**
   * Put the worker back to the availablity queue. 
   * If the worker process is dead, try replace with a new worker process.
   * If a new worker process cannot be started, decrease the pool size. 
   */
  void release_worker(std::unique_ptr<worker_process<ProxyType>>& worker) {
    logstream(LOG_DEBUG) << "Release worker " << worker->id << std::endl;
    std::unique_lock<graphlab::mutex> lck(m_mutex);
    if (check_alive(worker) == true) {
      // put the worker back to queue
      m_available_workers.push_back(std::move(worker));
    } else {
      logstream(LOG_WARNING) << "Replacing dead worker " << worker->id << std::endl;
      // clear dead worker
      worker.reset(nullptr);
      // start new worker
      auto new_worker = try_spawn_worker<ProxyType>(m_worker_binary_and_args, new_worker_address(), m_connection_timeout);
      if (new_worker != nullptr) {
        // put new worker back to queue
        m_available_workers.push_back(std::move(new_worker));
      } else {
        --m_num_workers;
        logstream(LOG_WARNING) << "Decrease number of workers to "
                               << m_num_workers << std::endl;
      }
    }
    lck.unlock();
    cv.notify_one();
  }

  /**
   * Return number of total workers in the pool.
   */
  size_t num_workers() const { return m_num_workers; };

  /**
   * Return number of avaiable workers in the pool.
   */
  size_t num_available_workers() {
    std::unique_lock<graphlab::mutex> lck(m_mutex);
    return m_available_workers.size();
  }

  /**
   * Call the function on all worker in parallel and return the results.
   * Block until all workers are avaiable.
   */
  template<typename RetType, typename Fn>
  std::vector<RetType> call_all_workers(Fn f) {
    std::unique_lock<graphlab::mutex> lck(m_mutex);
    wait_for_all(lck);
    // take out all workers from m_avaiable_workers
    // equivalent to call get_worker() in batch with lck acquired.
    std::vector<std::unique_ptr<worker_process<ProxyType>>> temp_workers;
    for (size_t i = 0; i < m_num_workers; ++i) {
      temp_workers.push_back(std::move(m_available_workers.front()));
      m_available_workers.pop_front();
    }

    // The following code calls release_worker() for crash recovery,
    // and release_worker() calls lock inside.
    // Because no workers is avaialable externally, we can safely release the lock.
    lck.unlock();

    try {
      std::vector<std::shared_ptr<worker_guard<ProxyType>>> guards;
      for (auto& worker: temp_workers)
        guards.push_back(get_worker_guard(worker));
      std::vector<RetType> ret(m_num_workers);
      parallel_for(0, m_num_workers, [&](size_t i) {
        try {
          ret[i] = f(temp_workers[i]->proxy);
        } catch (cppipc::ipcexception e) {
          throw reinterpret_comm_failure(e);
        }
      });
      return ret;
    } catch (...) {
      ASSERT_EQ(m_available_workers.size(), m_num_workers);
      std::rethrow_exception(std::current_exception());
    }
  }

  /// constructor
  worker_pool(size_t num_workers,
              std::vector<std::string> worker_binary_and_args,
              int connection_timeout = 3) {
    m_connection_timeout = connection_timeout;
    m_worker_binary_and_args = worker_binary_and_args;
    m_num_workers = 0;
    init(num_workers);
  }

  /// destructor
  ~worker_pool() {
    std::unique_lock<graphlab::mutex> lck(m_mutex);
    try {
      wait_for_all(lck);
    } catch (...) { }
    parallel_for(0, m_available_workers.size(), [&](size_t i) {
      m_available_workers[i].reset();
    });
  }

private:
  /**
   * Wait until all workers are returned. Throw error if pool size become 0.
   */
  void wait_for_all(std::unique_lock<graphlab::mutex>& lck) {
    while (m_available_workers.size() < m_num_workers || m_num_workers == 0) {
      cv.wait(lck);
    }
    if (m_num_workers == 0) {
      throw("Worker pool is empty");
    }
  }

  /**
   * Wait until a single worker is aviailable. Throw error if pool size become 0.
   */
  void wait_for_one(std::unique_lock<graphlab::mutex>& lck) {
    while (m_available_workers.empty() || m_num_workers == 0) {
      cv.wait(lck);
    }
    if (m_num_workers == 0) {
      throw("Worker pool is empty");
    }
  }

  /**
   * Return true if the worker process is alive.
   */
  bool check_alive(std::unique_ptr<worker_process<ProxyType>>& worker) {
    return (worker->process_ != nullptr) && worker->process_->exists();
  }

  /**
   * Return a unique ipc socket file address. 
   */
  std::string new_worker_address() {
    return "ipc://" + get_temp_name();
  }

  /**
   * Initialize the pool with N workers.
   */
  void init(size_t num_workers) {
    parallel_for(0, num_workers, [&](size_t i) {
      auto new_worker = try_spawn_worker<ProxyType>(m_worker_binary_and_args,
                                                    new_worker_address(),
                                                    m_connection_timeout);
      if (new_worker != nullptr) {
        std::unique_lock<graphlab::mutex> lck(m_mutex);
        m_available_workers.push_back(std::move(new_worker));
        ++m_num_workers;
      }
    });

    if (m_num_workers == 0) {
      log_and_throw("Cannot evaluate lambda. Lambda workers cannot not start.");
    } else if (m_num_workers < num_workers) {
      logprogress_stream << "Less than " << num_workers << " successfully started. "
                         << "Using only " << m_num_workers  << " workers." << std::endl;
      logprogress_stream << "All operations will proceed as normal, but "
                         << "lambda operations will not be able to use all "
                         << "available cores." << std::endl;
      logprogress_stream << "To help us diagnose this issue, please send "
                         << "the log file to product-feedback@dato.com." << std::endl;
      logprogress_stream << "(The location of the log file is printed at the "
                         << "start of the GraphLab server)."  << std::endl;
      logstream(LOG_ERROR) << "Less than " << num_workers << " successfully started."
                           << "Using only " << m_num_workers << std::endl;
    }
  }

private:
  std::vector<std::string> m_worker_binary_and_args;
  int m_connection_timeout;
  std::deque<std::unique_ptr<worker_process<ProxyType>>> m_available_workers;
  size_t m_num_workers;
  graphlab::condition_variable cv;
  graphlab::mutex m_mutex;
}; // end of worker_pool


/**
 * RAII for allocating worker proxy.
 * When the worker_guard is destoryed, the guarded worker
 * be released by the worker_pool.
 */
template<typename ProxyType>
class worker_guard {
 public:
   worker_guard(worker_pool<ProxyType>* worker_pool_,
                std::unique_ptr<worker_process<ProxyType>>& worker_process_)
     : m_pool(worker_pool_), m_worker(worker_process_) { }

   ~worker_guard() {
     m_pool->release_worker(m_worker);
   }

 private:
   worker_pool<ProxyType>* m_pool;
   std::unique_ptr<worker_process<ProxyType>>& m_worker;
};

} // end of lambda
} // end of graphlab
#endif
