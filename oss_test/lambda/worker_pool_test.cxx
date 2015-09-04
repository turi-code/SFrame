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
#include "dummy_worker_interface.hpp"

using namespace graphlab;

class worker_pool_test: public CxxTest::TestSuite {
 public:
  void test_spawn_workers() {
    size_t nworkers = 3;
    auto wk_pool = get_worker_pool(nworkers);
    TS_ASSERT_EQUALS(wk_pool->num_workers(), nworkers);
    TS_ASSERT_EQUALS(wk_pool->num_available_workers(), nworkers);
  }

  void test_get_and_release_worker() {
    size_t nworkers = 3;
    auto wk_pool = get_worker_pool(nworkers);
    parallel_for(0, (size_t)16, [&](size_t i) {
      std::string message = std::to_string(i);
      auto proxy = wk_pool->get_worker();
      TS_ASSERT(proxy->echo(message).compare(message) == 0);
      wk_pool->release_worker(proxy);
    });
  }

  void test_worker_guard() {
    size_t nworkers = 3;
    auto wk_pool = get_worker_pool(nworkers);
    parallel_for(0, (size_t)16, [&](size_t i) {
      std::string message = std::to_string(i);
      auto proxy = wk_pool->get_worker();
      auto guard = wk_pool->get_worker_guard(proxy);
      TS_ASSERT(proxy->echo(message).compare(message) == 0);
      TS_ASSERT_THROWS_ANYTHING(proxy->throw_error());
    });
  }

  void test_worker_crash_and_restart() {
    size_t nworkers = 3;
    auto wk_pool = get_worker_pool(nworkers);
    {
      auto proxy = wk_pool->get_worker();
      auto guard = wk_pool->get_worker_guard(proxy);
      TS_ASSERT_THROWS(proxy->quit(0), cppipc::ipcexception);
    }
    parallel_for(0, (size_t)6, [&](size_t i) {
      std::string message = std::to_string(i);
      auto proxy = wk_pool->get_worker();
      auto guard = wk_pool->get_worker_guard(proxy);
      TS_ASSERT(proxy->echo(message).compare(message) == 0);
      TS_ASSERT_THROWS(proxy->quit(0), cppipc::ipcexception);
    });

    TS_ASSERT_EQUALS(wk_pool->num_workers(), nworkers);
    TS_ASSERT_EQUALS(wk_pool->num_available_workers(), nworkers);

    parallel_for(0, (size_t)6, [&](size_t i) {
       std::string message = std::to_string(i);
       auto proxy = wk_pool->get_worker();
       auto guard = wk_pool->get_worker_guard(proxy);
       TS_ASSERT(proxy->echo(message).compare(message) == 0);
     });
  }

 private:
  std::shared_ptr<lambda::worker_pool<dummy_worker_proxy>> get_worker_pool(size_t poolsize) {
    int timeout = 1;
    return std::make_shared<lambda::worker_pool<dummy_worker_proxy>>(
        poolsize, worker_binary,
        std::vector<std::string>(), timeout);
  };

 private:
#ifndef _WIN32
  const std::string worker_binary = "./dummy_worker";
#else
  const std::string worker_binary = "./dummy_worker.exe";
#endif
};
