/*
* Copyright (C) 2016 Turi
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
#include <fstream>
#include <logger/logger.hpp>
#include <logger/log_rotate.hpp>
#include <logger/log_level_setter.hpp>
#include <cxxtest/TestSuite.h>
using namespace graphlab;

class logger_test: public CxxTest::TestSuite {
 public:
  void test_empty_log() {
    global_logger().set_log_level(LOG_INFO);
    logstream(LOG_INFO) << "\n";
    logstream(LOG_INFO);
    logstream(LOG_INFO);
    logstream(LOG_INFO) << std::endl;
  }

  void test_log_level_setter() {
    logprogress_stream << "This should show up" << std::endl;
    auto x = log_level_setter(LOG_NONE);
    logprogress_stream << "This should not print." << std::endl;
  }

  void test_log_rotation() {
    global_logger().set_log_level(LOG_INFO);
    begin_log_rotation("rotate.log",
                       1 /* log rotates every second */,
                       2 /* we only keep the last 2 logs around*/);
    for (size_t i = 0;i < 5; ++i) {
      logstream(LOG_INFO) << i << std::endl;
      timer::sleep(1);
    }
    TS_ASSERT(std::ifstream("rotate.log").good());
    TS_ASSERT(std::ifstream("rotate.log.0").good() == false);
    TS_ASSERT(std::ifstream("rotate.log.1").good() == false);
    stop_log_rotation();
  }
};
