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
#include <cxxtest/TestSuite.h>
#include <fileio/s3_api.hpp>

class s3api_test: public CxxTest::TestSuite {
 public:


  void test_parse_s3url(void) {

    webstor::s3url out; // {access_key_id, secret_key, bucket, object_name, endpoint}
    webstor::s3url expected;

    out = {};
    TS_ASSERT(webstor::parse_s3url("s3://::foo/bar", out));
    expected = {"", "", "foo", "bar", ""};
    TS_ASSERT_EQUALS(out, expected);

    out = {};
    TS_ASSERT(webstor::parse_s3url("s3://id:key:foo/bar", out));
    expected = {"id", "key", "foo", "bar", ""};
    TS_ASSERT_EQUALS(out, expected);

    out = {};
    TS_ASSERT(webstor::parse_s3url("s3://id:key:s3.amazonaws.com/foo/bar", out));
    expected = {"id", "key", "foo", "bar", "s3.amazonaws.com"};
    TS_ASSERT_EQUALS(out, expected);

    out = {};
    TS_ASSERT(webstor::parse_s3url("s3://id:key:s3.amazonaws.com/foo.123.xyz-pikachu/1:::,/2'/3\\/4", out));
    expected = {"id", "key", "foo.123.xyz-pikachu", "1:::,/2'/3\\/4", "s3.amazonaws.com"};
    TS_ASSERT_EQUALS(out, expected);

    out = {};
    TS_ASSERT(webstor::parse_s3url("s3://id:key:gl-rv-test/psone_logs/2014-12-11T18:40:40.Roberts-MacBook-Pro.local_server.log", out));
    expected = {"id", "key", "gl-rv-test", "psone_logs/2014-12-11T18:40:40.Roberts-MacBook-Pro.local_server.log", ""};
    TS_ASSERT_EQUALS(out, expected);

    // missing both id and key
    TS_ASSERT(!webstor::parse_s3url("s3://foo/bar", out));

    // missing one of the id and key
    TS_ASSERT(!webstor::parse_s3url("s3://key:foo/bar", out));

    // bad bucket name
    TS_ASSERT(webstor::parse_s3url("s3://::AAA/bar", out)); // capital letter
    TS_ASSERT(!webstor::parse_s3url("s3://::abc-/bar", out)); // hyphen end
    TS_ASSERT(!webstor::parse_s3url("s3://::-abc/bar", out)); // hyphen begin
    TS_ASSERT(!webstor::parse_s3url("s3://::a./bar", out)); // end dot 
    TS_ASSERT(!webstor::parse_s3url("s3://::.a/bar", out)); // begin dot 
    TS_ASSERT(!webstor::parse_s3url("s3://::a/bar", out)); // too short
    TS_ASSERT(!webstor::parse_s3url("s3://::10.10.10.10/bar", out)); // ip address
    TS_ASSERT(webstor::parse_s3url("s3://::GraphLab-Dataset/bar", out)); // hyphen end
  }
};
