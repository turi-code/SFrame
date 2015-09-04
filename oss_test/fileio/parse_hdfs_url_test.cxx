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
#include <cxxtest/TestSuite.h>
#include <fileio/fs_utils.hpp>

using namespace graphlab::fileio;

class hdfs_parse_url_test: public CxxTest::TestSuite {
 public:

  void test_default() { 
    auto input = "hdfs:///foo/bar/a.txt";
    auto expected = std::make_tuple(default_host, default_port, "/foo/bar/a.txt");
    auto actual = parse_hdfs_url(input);
    TS_ASSERT_EQUALS(std::get<0>(actual), std::get<0>(expected));
    TS_ASSERT_EQUALS(std::get<1>(actual), std::get<1>(expected));
    TS_ASSERT_EQUALS(std::get<2>(actual), std::get<2>(expected));
  }

  void test_hostname() {
    auto input = "hdfs://hostname/foo/bar/a.txt";
    auto expected = std::make_tuple("hostname", default_port, "/foo/bar/a.txt");
    auto actual = parse_hdfs_url(input);
    TS_ASSERT_EQUALS(std::get<0>(actual), std::get<0>(expected));
    TS_ASSERT_EQUALS(std::get<1>(actual), std::get<1>(expected));
    TS_ASSERT_EQUALS(std::get<2>(actual), std::get<2>(expected));
  }

  void test_hostname_and_port() { 
    auto input = "hdfs://hostname:9000/foo/bar/a.txt";
    auto expected = std::make_tuple("hostname", "9000", "/foo/bar/a.txt");
    auto actual = parse_hdfs_url(input);
    TS_ASSERT_EQUALS(std::get<0>(actual), std::get<0>(expected));
    TS_ASSERT_EQUALS(std::get<1>(actual), std::get<1>(expected));
    TS_ASSERT_EQUALS(std::get<2>(actual), std::get<2>(expected));
  }

  void test_ip_hostname() {
    auto input = "hdfs://10.10.10.10/foo/bar/a.txt";
    auto expected = std::make_tuple("10.10.10.10", default_port, "/foo/bar/a.txt");
    auto actual = parse_hdfs_url(input);
    TS_ASSERT_EQUALS(std::get<0>(actual), std::get<0>(expected));
    TS_ASSERT_EQUALS(std::get<1>(actual), std::get<1>(expected));
    TS_ASSERT_EQUALS(std::get<2>(actual), std::get<2>(expected));
  }

  void test_ip_hostname_and_port() {
    auto input = "hdfs://10.10.10.10:9000/foo/bar/a.txt";
    auto expected = std::make_tuple("10.10.10.10", "9000", "/foo/bar/a.txt");
    auto actual = parse_hdfs_url(input);
    TS_ASSERT_EQUALS(std::get<0>(actual), std::get<0>(expected));
    TS_ASSERT_EQUALS(std::get<1>(actual), std::get<1>(expected));
    TS_ASSERT_EQUALS(std::get<2>(actual), std::get<2>(expected));
  }

  void test_empty_exception() {
    auto input = "hdfs://a";
    auto actual = parse_hdfs_url(input);
    auto expected = default_expected;
    TS_ASSERT_EQUALS(std::get<0>(actual), std::get<0>(expected));
    TS_ASSERT_EQUALS(std::get<1>(actual), std::get<1>(expected));
    TS_ASSERT_EQUALS(std::get<2>(actual), std::get<2>(expected));
  }

  void test_bad_path_exception() {
    auto input = "hdfs://hostname:10000/foo:bar";
    auto expected = default_expected;
    auto actual = parse_hdfs_url(input);
    TS_ASSERT_EQUALS(std::get<0>(actual), std::get<0>(expected));
    TS_ASSERT_EQUALS(std::get<1>(actual), std::get<1>(expected));
    TS_ASSERT_EQUALS(std::get<2>(actual), std::get<2>(expected));
  }

  void test_bad_port_exception() {
    auto input = "hdfs://hostname:badport/foo/bar";
    auto expected = default_expected;
    auto actual = parse_hdfs_url(input);
    TS_ASSERT_EQUALS(std::get<0>(actual), std::get<0>(expected));
    TS_ASSERT_EQUALS(std::get<1>(actual), std::get<1>(expected));
    TS_ASSERT_EQUALS(std::get<2>(actual), std::get<2>(expected));
  }

private:

  const std::string default_port = "0";
  const std::string default_host = "default";
  const std::tuple<std::string, std::string, std::string> default_expected = std::make_tuple(default_host, default_port, "");
};
