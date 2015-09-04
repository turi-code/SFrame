/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <lambda/lambda_master.hpp>
#include <lambda/lambda_utils.hpp>
#include <parallel/lambda_omp.hpp>
#include <fileio/temp_files.hpp>
#include <algorithm>
#include <lambda/lambda_constants.hpp>
#include <shmipc/shmipc.hpp>

namespace graphlab {
namespace lambda {

  // Path of the lambda_worker binary relative to the unity_server binary.
  // The relative path will be set when unity_server starts.
#if _WIN32
  std::string lambda_master::lambda_worker_binary = "pylambda_worker.exe";
#else
  std::string lambda_master::lambda_worker_binary = "pylambda_worker";
#endif
  graphlab::mutex get_instance_lock;

  lambda_master& lambda_master::get_instance() {
    std::lock_guard<graphlab::mutex> lock(get_instance_lock);
    static lambda_master instance(std::min<size_t>(DEFAULT_NUM_PYLAMBDA_WORKERS,
                                                     std::max<size_t>(thread::cpu_count(), 1)));
    return instance;
  };

  lambda_master::lambda_master(size_t nworkers) {
    std::vector<std::string> worker_addresses;
    for (size_t i = 0; i < nworkers; ++i) {
      worker_addresses.push_back(std::string("ipc://") + get_temp_name());
    }
    m_worker_pool.reset(new worker_pool<lambda_evaluator_proxy>(nworkers,
                                                                lambda_worker_binary,
                                                                worker_addresses));
    if (nworkers < thread::cpu_count()) {
      logprogress_stream << "Using default " << nworkers << " lambda workers.\n";
      logprogress_stream << "To maximize the degree of parallelism, add the following code to the beginning of the program:\n";
      logprogress_stream << "\"graphlab.set_runtime_config(\'GRAPHLAB_DEFAULT_NUM_PYLAMBDA_WORKERS\', " << thread::cpu_count() << ")\"\n";
      logprogress_stream << "Note that increasing the degree of parallelism also increases the memory footprint." << std::endl;
    }

    /*
     * Create an interprocess shared memory connection if possible.
     */
    auto shared_memory_setup = [](std::shared_ptr<lambda_evaluator_proxy> proxy) {
      return std::make_pair((void*)(proxy.get()), proxy->initialize_shared_memory_comm());
    };
    std::vector<std::pair<void*, std::string>> shared_memory_addresses = 
        m_worker_pool->call_all_workers<std::pair<void*, std::string>>(shared_memory_setup);

    for (auto shared_memory_address: shared_memory_addresses) {
      // for each worker, try to connect and store the addresses
      if (!shared_memory_address.second.empty()) {
        std::shared_ptr<shmipc::client> client = std::make_shared<shmipc::client>();
        if (client->connect(shared_memory_address.second)) {
          m_shared_memory_worker_connections[shared_memory_address.first] = client;
        }
      }
    }
  }

  size_t lambda_master::make_lambda(const std::string& lambda_str) {
    std::lock_guard<graphlab::mutex> lock(m_mtx);
    auto make_lambda_fn = [lambda_str](std::shared_ptr<lambda_evaluator_proxy> proxy) {
      try {
        auto ret = proxy->make_lambda(lambda_str);
        logstream(LOG_INFO) << "Lambda worker proxy make lambda: " << ret << std::endl;
        return ret;
      } catch (cppipc::ipcexception e) {
        throw reinterpret_comm_failure(e);
      }
    };
    std::vector<size_t> returned_hashes = m_worker_pool->call_all_workers<size_t>(make_lambda_fn);
    // validate all worker returns the same hash
    size_t lambda_hash = returned_hashes[0];
    for (auto& v : returned_hashes) {
      DASSERT_MSG(lambda_hash == v,
                  "workers should return the same lambda index");
    }
    m_lambda_object_counter[lambda_hash]++;
    return lambda_hash;
  }

  void lambda_master::release_lambda(size_t lambda_hash) noexcept {
    // Check the counter to see if the lambda is unique
    std::lock_guard<graphlab::mutex> lock(m_mtx);
    {
      if (m_lambda_object_counter.find(lambda_hash) == m_lambda_object_counter.end()) {
        return;
      }
      m_lambda_object_counter[lambda_hash]--;
      if (m_lambda_object_counter[lambda_hash] > 0) {
        return;
      }
    }

    // Ok, the lambda is unique, let's issue a release lambda to all workers
    auto release_lambda_fn = [lambda_hash](std::shared_ptr<lambda_evaluator_proxy> proxy) {
      try {
        proxy->release_lambda(lambda_hash);
      } catch (std::exception e){
        logstream(LOG_ERROR) << "Error on releasing lambda: " << e.what() << std::endl;
      } catch (std::string e) {
        logstream(LOG_ERROR) << "Error on releasing lambda: " << e << std::endl;
      } catch (const char* e) {
        logstream(LOG_ERROR) << "Error on releasing lambda: " << e << std::endl;
      } catch (...) {
        logstream(LOG_ERROR) << "Error on releasing lambda: Unknown error" << std::endl;
      }
      return 0;
    };
    m_worker_pool->call_all_workers<size_t>(release_lambda_fn);
  }


  void lambda_master::bulk_eval(size_t lambda_hash,
                                  const std::vector<flexible_type>& args,
                                  std::vector<flexible_type>& out,
                                  bool skip_undefined, int seed) {

    auto worker_proxy = m_worker_pool->get_worker();
    auto worker_guard = m_worker_pool->get_worker_guard(worker_proxy);
    // catch and reinterpret comm failure
    try {
      out = worker_proxy->bulk_eval(lambda_hash, args, skip_undefined, seed);
    } catch (cppipc::ipcexception e) {
      throw reinterpret_comm_failure(e);
    }
  }


  /**
   * Performs a remote call to an interprocess shared memory server
   * deserializing the response to RetType.
   *
   * Note that this is not a general purpose function and only works 
   * with pylambda_master and pylambda_evaluator.
   *
   * Performs a remote call for bulk_eval_rows and bulk_eval_dict_rows.
   *
   * Arguments must be serialized into the "arguments" archive. This function
   * will also take over management of the buffer inside of "arguments" and
   * free it when done.
   *
   * Results will be deserialized into the ret.
   *
   * This function may throw exceptions if remote exceptions were raised.
   */
  template <typename RetType>
  static bool shm_call(const std::shared_ptr<shmipc::client>& shmclient,
                       oarchive& arguments,
                       RetType& ret) {
    // send the message
    bool shmok = shmipc::large_send(*shmclient, arguments.buf, arguments.off);
    if (shmok == false) {
      free(arguments.buf); arguments.buf = nullptr;
    }

    // reuse the arguments buffer so we dont alloc again
    // receive the reply
    char* buf = arguments.buf;
    size_t buflen = arguments.len;
    size_t receivelen = 0;
    arguments.buf = nullptr;
    shmok = shmipc::large_receive(*shmclient, &buf, &buflen, 
                                  receivelen, (size_t)(-1));
    if (shmok == false) {
      free(buf);
      return false;
    }

    // deserialize
    // first byte is whether it is an error message or not
    iarchive iarc(buf, receivelen);
    char good_call;
    iarc >> good_call;
    if (good_call) {
      iarc >> ret;
    } else {
      std::string message;
      iarc >> message;
      throw message;
    }
    free(buf);

    return true;
  }


  /**
   * \overload with sframe rows
   */
  void lambda_master::bulk_eval(size_t lambda_hash,
                                  const sframe_rows& args,
                                  std::vector<flexible_type>& out,
                                  bool skip_undefined,
                                  int seed) {

    auto worker_proxy = m_worker_pool->get_worker();
    auto worker_guard = m_worker_pool->get_worker_guard(worker_proxy);

    // catch and reinterpret comm failure
    try {
      auto shmclient_iter = m_shared_memory_worker_connections.find(worker_proxy.get());
      if (shmclient_iter != m_shared_memory_worker_connections.end() &&
          shmclient_iter->second.get() != nullptr) {
        auto& shmclient = shmclient_iter->second;
        oarchive oarc;
        oarc << (char)(bulk_eval_serialized_tag::BULK_EVAL_ROWS)
             << lambda_hash
             << args
             << skip_undefined
             << seed;
        bool good = shm_call(shmclient, oarc, out);
        // if shmcall was good, return. 
        if (good) return;

        // otherwise shmcall was bad. reset the client so we don't ever use
        // it again and fall back to regular IPC.
        // (note. we cannot delete it from the
        // m_shared_memory_worker_connections map because of concurency 
        // issues. There may be parallel access to it and locking seems 
        // overkill.
        shmclient.reset();
        logstream(LOG_WARNING) << "Unexpected SHMIPC failure. Falling back to CPPIPC" << std::endl;
      } 
      out = worker_proxy->bulk_eval_rows(lambda_hash, args, skip_undefined, seed);
    } catch (cppipc::ipcexception e) {
      throw reinterpret_comm_failure(e);
    }
  }


  void lambda_master::bulk_eval(size_t lambda_hash,
                                  const std::vector<std::string>& keys,
                                  const std::vector<std::vector<flexible_type>>& values,
                                  std::vector<flexible_type>& out,
                                  bool skip_undefined, int seed) {
    auto worker_proxy = m_worker_pool->get_worker();
    auto worker_guard = m_worker_pool->get_worker_guard(worker_proxy);
    // catch and reinterpret comm failure
    try {
      out = worker_proxy->bulk_eval_dict(lambda_hash, keys, values, skip_undefined, seed);
    } catch (cppipc::ipcexception e) {
      throw reinterpret_comm_failure(e);
    }
  }


  void lambda_master::bulk_eval(size_t lambda_hash,
                                  const std::vector<std::string>& keys,
                                  const sframe_rows& rows,
                                  std::vector<flexible_type>& out,
                                  bool skip_undefined, int seed) {
    auto worker_proxy = m_worker_pool->get_worker();
    auto worker_guard = m_worker_pool->get_worker_guard(worker_proxy);
    // catch and reinterpret comm failure
    try {
      auto shmclient_iter = m_shared_memory_worker_connections.find(worker_proxy.get());
      if (shmclient_iter != m_shared_memory_worker_connections.end() &&
          shmclient_iter->second.get() != nullptr) {
        auto& shmclient = shmclient_iter->second;
        oarchive oarc;
        oarc << (char)(bulk_eval_serialized_tag::BULK_EVAL_DICT_ROWS)
             << lambda_hash
             << keys 
             << rows
             << skip_undefined
             << seed;
        bool good = shm_call(shmclient, oarc, out);
        // everything good. return
        if (good) return;


        // shmcall was bad... reset the client so we don't ever use it again
        // and fall back to regular IPC
        shmclient.reset();
        logstream(LOG_WARNING) << "Unexpected SHMIPC failure. Falling back to CPPIPC" << std::endl;
      } 
      out = worker_proxy->bulk_eval_dict_rows(lambda_hash, keys, rows, skip_undefined, seed);
    } catch (cppipc::ipcexception e) {
      throw reinterpret_comm_failure(e);
    }
  }
} // end of lambda
} // end of graphlab
