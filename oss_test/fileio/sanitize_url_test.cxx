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
#include <fileio/sanitize_url.hpp>

using namespace graphlab;

class sanitize_url_test: public CxxTest::TestSuite {
 public:
  void test_sanitize_url(void) {
    TS_ASSERT_EQUALS(sanitize_url("http://www.google.com"), "http://www.google.com");
    TS_ASSERT_EQUALS(sanitize_url("file://www.google.com"), "file://www.google.com");
    TS_ASSERT_EQUALS(sanitize_url("hdfs://hello:world@www.google.com"), "hdfs://hello:world@www.google.com");
    TS_ASSERT_EQUALS(sanitize_url("s3://aa:pika/chu"), "s3://pika/chu");
    TS_ASSERT_EQUALS(sanitize_url("s3://aa:bb:pika/chu"), "s3://pika/chu");
    TS_ASSERT_EQUALS(sanitize_url("s3://aa:bb:s3.amazonaws.com/pika/chu"), "s3://s3.amazonaws.com/pika/chu");
    TS_ASSERT_EQUALS(sanitize_url("s3://a/a:bb:cc:pika/chu"), "s3://pika/chu");
    TS_ASSERT_EQUALS(sanitize_url("s3://a/a:bb:cc:s3.amazonaws.com/pika/chu"), "s3://s3.amazonaws.com/pika/chu");
    TS_ASSERT_EQUALS(sanitize_url("s3://a/a:b/b:cc:pika/chu"), "s3://pika/chu");
    TS_ASSERT_EQUALS(sanitize_url("s3://a/a:b/b:cc:s3.amazonaws.com/pika/chu"), "s3://s3.amazonaws.com/pika/chu");
    TS_ASSERT_EQUALS(sanitize_url("s3://:pika/chu"), "s3://pika/chu");
    TS_ASSERT_EQUALS(sanitize_url("s3://:s3.amazonaws.com/pika/chu"), "s3://s3.amazonaws.com/pika/chu");
    TS_ASSERT_EQUALS(sanitize_url("s3://:::pika/chu"), "s3://pika/chu");
    TS_ASSERT_EQUALS(sanitize_url("s3://:::s3.amazonaws.com/pika/chu"), "s3://s3.amazonaws.com/pika/chu");
  }
};
