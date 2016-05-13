/*
* Copyright (C) 2015 Dato, Inc.
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Affero General Public License as
* published by the Free Software Foundation, either version 3 of the
* License, or (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU Affero General Public License for more details.
*
* You should have received a copy of the GNU Affero General Public License
* along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/
#include <iostream>
#include <cxxtest/TestSuite.h>
#include <lambda/worker_pool.hpp>
#include <parallel/lambda_omp.hpp>
#include <fileio/fs_utils.hpp>
#include <fault/sockets/socket_config.hpp>
#include "dummy_worker_interface.hpp"

using namespace graphlab;

class worker_pool_test: public CxxTest::TestSuite {
 public:
  worker_pool_test() {
    global_logger().set_log_level(LOG_INFO);

    // Manually set this one.
    char* use_fallback = std::getenv("GRAPHLAB_FORCE_IPC_TO_TCP_FALLBACK");
    if(use_fallback != nullptr && std::string(use_fallback) == "1") {
      libfault::FORCE_IPC_TO_TCP_FALLBACK = true;
    }
  }
  void test_spawn_workers() {
    auto wk_pool = get_worker_pool(nworkers);
    TS_ASSERT_EQUALS(wk_pool->num_workers(), nworkers);
    TS_ASSERT_EQUALS(wk_pool->num_available_workers(), nworkers);
  }

  void test_get_and_release_worker() {
    auto wk_pool = get_worker_pool(nworkers);
    parallel_for(0, (size_t)16, [&](size_t i) {
      std::string message = std::to_string(i);
      auto worker = wk_pool->get_worker();
      TS_ASSERT(worker->proxy->echo(message).compare(message) == 0);
      wk_pool->release_worker(worker);
    });
  }

  void test_worker_guard() {
    auto wk_pool = get_worker_pool(nworkers);
    parallel_for(0, nworkers * 4, [&](size_t i) {
      std::string message = std::to_string(i);
      auto worker = wk_pool->get_worker();
      auto guard = wk_pool->get_worker_guard(worker);
      TS_ASSERT(worker->proxy->echo(message).compare(message) == 0);
      TS_ASSERT_THROWS_ANYTHING(worker->proxy->throw_error());
    });
  }

  void test_worker_crash_and_restart() {
    auto wk_pool = get_worker_pool(nworkers);
    {
      auto worker = wk_pool->get_worker();
      auto guard = wk_pool->get_worker_guard(worker);
      TS_ASSERT_THROWS(worker->proxy->quit(0), cppipc::ipcexception);
    }
    TS_ASSERT_EQUALS(wk_pool->num_workers(), nworkers);

    parallel_for(0, nworkers, [&](size_t i) {
      std::string message = std::to_string(i);
      auto worker = wk_pool->get_worker();
      auto guard = wk_pool->get_worker_guard(worker);
      TS_ASSERT(worker->proxy->echo(message).compare(message) == 0);
      TS_ASSERT_THROWS(worker->proxy->quit(0), cppipc::ipcexception);
    });

    TS_ASSERT_EQUALS(wk_pool->num_workers(), nworkers);
    TS_ASSERT_EQUALS(wk_pool->num_available_workers(), nworkers);

    parallel_for(0, nworkers, [&](size_t i) {
       std::string message = std::to_string(i);
       auto worker = wk_pool->get_worker();
       auto guard = wk_pool->get_worker_guard(worker);
       TS_ASSERT(worker->proxy->echo(message).compare(message) == 0);
     });
  }

  void test_call_all_workers() {
    auto wk_pool = get_worker_pool(nworkers);
    auto f = [](std::unique_ptr<dummy_worker_proxy>& proxy) {
      proxy->echo("");
      return 0;
    };
    auto ret = wk_pool->call_all_workers<int>(f);
    TS_ASSERT_EQUALS(ret.size(), nworkers);
  }

  void test_call_all_workers_with_exception() {
    auto wk_pool = get_worker_pool(nworkers);
    auto f = [](std::unique_ptr<dummy_worker_proxy>& proxy) {
      proxy->throw_error();
      return 0;
    };
    TS_ASSERT_THROWS_ANYTHING(wk_pool->call_all_workers<int>(f));
    TS_ASSERT_EQUALS(wk_pool->num_workers(), nworkers);
    TS_ASSERT_EQUALS(wk_pool->num_available_workers(), nworkers);
  }

  void test_call_all_workers_with_crash_recovery() {
    auto wk_pool = get_worker_pool(nworkers);
    auto bad_fun = [](std::unique_ptr<dummy_worker_proxy>& proxy) {
      proxy->quit(0);
      return 0;
    };
    TS_ASSERT_THROWS(wk_pool->call_all_workers<int>(bad_fun), cppipc::ipcexception);

    // call_all_worker should recover after crash
    TS_ASSERT_EQUALS(wk_pool->num_workers(), nworkers);
    TS_ASSERT_EQUALS(wk_pool->num_available_workers(), nworkers);

    auto good_fun = [](std::unique_ptr<dummy_worker_proxy>& proxy) {
      proxy->echo("");
      return 0;
    };
    TS_ASSERT_EQUALS(wk_pool->call_all_workers<int>(good_fun).size(), nworkers);
  }

 private:
  std::shared_ptr<lambda::worker_pool<dummy_worker_proxy>> get_worker_pool(size_t poolsize) {
    int timeout = 1;
    std::shared_ptr<lambda::worker_pool<dummy_worker_proxy>> ret;
    ret.reset(new lambda::worker_pool<dummy_worker_proxy>(poolsize, {worker_binary}, timeout));
    return ret;
  };

 private:
  size_t nworkers = 3;
#ifndef _WIN32
  const std::string worker_binary = "./dummy_worker";
#else
  const std::string worker_binary = "./dummy_worker.exe";
#endif
};
