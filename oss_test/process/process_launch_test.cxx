/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <process/process.hpp>
#include <cxxtest/TestSuite.h>
#include <thread>

using namespace graphlab;

#ifdef _WIN32
bool windows = true;
#else
bool windows = false;
#endif

class process_launch_test: public CxxTest::TestSuite {
 public:
  void test_basic_launch() {
    process p;

    std::string proc_name("./sleepy_process");
    if(windows)
      proc_name += std::string(".exe");

    auto ret = p.launch(proc_name, std::vector<std::string>{});
    TS_ASSERT(ret);

    bool exists = p.exists();
    TS_ASSERT(exists);
  }

  void test_read_from_stdout() {
    process p;

    std::string proc_name("./hello");
    if(windows)
      proc_name += std::string(".exe");

    auto ret = p.popen(proc_name, std::vector<std::string>{"55","83","41"}, STDOUT_FILENO);
    TS_ASSERT(ret);

    char *buf = new char[4096];
    memset(buf, 0, 4096);
    char *buf_ptr = buf;
    ssize_t bytes_read = 0;
    while(bytes_read < 4096) {
      // Test reading in small chunks
      ssize_t bytes_returned = p.read_from_child(buf_ptr, 4);
      if(bytes_returned > 0) {
        bytes_read += bytes_returned;
        buf_ptr += bytes_returned;
      } else {
        break;
      }
    }

    std::string validate_str("Hello world! 55 83 41 ");

    validate_str = validate_str.insert(12, std::string(" ") + proc_name);

    TS_ASSERT_SAME_DATA(buf, validate_str.c_str(), validate_str.size());

    delete[] buf;
  }

  void test_kill() {
    process p;
    std::string proc_name("./sleepy_process");
    if(windows)
      proc_name += std::string(".exe");

    p.launch(proc_name, std::vector<std::string>{});
    TS_ASSERT_EQUALS(p.exists(), true);

    // Synchronously kill
    p.kill(false);
    TS_ASSERT_EQUALS(p.exists(), false);

  }

  void test_error_cases() {
    process p;
    std::string proc_name("./hello");
    if(windows)
      proc_name += std::string(".exe");

    TS_ASSERT_THROWS_ANYTHING(p.kill());
    TS_ASSERT_THROWS_ANYTHING(p.read_from_child(NULL, 0));
    TS_ASSERT_THROWS_ANYTHING(p.exists());

    char *buf = new char[4096];
    p.launch(proc_name, std::vector<std::string>{});
    TS_ASSERT_THROWS_ANYTHING(p.read_from_child(buf, 4));

    delete[] buf;
  }
};
