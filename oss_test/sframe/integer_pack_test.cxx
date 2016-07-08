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
#include <logger/logger.hpp>
#include <sframe/integer_pack.hpp>
#include <serialization/serialization_includes.hpp>
#include <cxxtest/TestSuite.h>
using namespace graphlab;
using namespace integer_pack;
class integer_pack_test: public CxxTest::TestSuite {
 public:
  void test_variable_code() {
    for (size_t shift = 0; shift < 64; shift += 8) {
      for (uint64_t i = 0; i < 256; ++i) {
        oarchive oarc;
        variable_encode(oarc, i << shift);
        uint64_t j;
        iarchive iarc(oarc.buf, oarc.off);
        variable_decode(iarc, j);
        TS_ASSERT_EQUALS(oarc.off, iarc.off);
        free(oarc.buf);
        TS_ASSERT_EQUALS(i << shift, j);
      }
    }
  }
  void test_pack() {
    {
      size_t len = 8;
      uint64_t in[8] = {19,20,21,22,23,24,25,26};
      uint64_t out[8];
      oarchive oarc;
      frame_of_reference_encode_128(in, 8, oarc);

      iarchive iarc(oarc.buf, oarc.off);
      frame_of_reference_decode_128(iarc, 8, out);
      TS_ASSERT_EQUALS(oarc.off, iarc.off);
      free(oarc.buf);

      for (size_t i = 0;i < len; ++i) {
        TS_ASSERT_EQUALS(in[i], out[i]);
      }
    }
    // simple cases
    for (size_t mod = 1; mod < 63; ++mod) {
      for (size_t len = 0; len <= 128; ++len) {
        uint64_t in[len];
        uint64_t out[len];
        for (size_t i = 0;i < len; ++i) {
          if (mod == 0) {
            in[i] = 0;
          } else {
            in[i] = (i % mod) & (1 << (mod - 1));
          }
        }
        oarchive oarc;
        frame_of_reference_encode_128(in, len, oarc);

        iarchive iarc(oarc.buf, oarc.off);
        frame_of_reference_decode_128(iarc, len, out);
        TS_ASSERT_EQUALS(oarc.off, iarc.off);
        free(oarc.buf);

        for (size_t i = 0;i < len; ++i) {
          if (in[i] != out[i]) std::cout << mod << " " << len << " " << i << "\n";
          TS_ASSERT_EQUALS(in[i], out[i]);
        }
      }
    }
    
    // harder cases
    for (size_t multiplier = 1; multiplier < 63; ++multiplier) {
      for (size_t shift = 1; shift < 63; ++shift) {
        size_t len = 128;
        uint64_t in[len];
        uint64_t out[len];
        for (size_t i = 0;i < len; ++i) {
          in[i] = shift + (multiplier * i);
        }
        oarchive oarc;
        frame_of_reference_encode_128(in, len, oarc);

        iarchive iarc(oarc.buf, oarc.off);
        frame_of_reference_decode_128(iarc, len, out);
        TS_ASSERT_EQUALS(oarc.off, iarc.off);
        free(oarc.buf);

        for (size_t i = 0;i < len; ++i) {
          TS_ASSERT_EQUALS(in[i], out[i]);
        }
      }
      for (size_t mod = 1; mod < 63; ++mod) {
        size_t len = 128;
        uint64_t in[len];
        uint64_t out[len];
        for (size_t i = 0;i < len; ++i) {
          in[i] = (multiplier * i) % mod;
        }
        oarchive oarc;
        frame_of_reference_encode_128(in, len, oarc);

        iarchive iarc(oarc.buf, oarc.off);
        frame_of_reference_decode_128(iarc, len, out);
        TS_ASSERT_EQUALS(oarc.off, iarc.off);
        free(oarc.buf);

        for (size_t i = 0;i < len; ++i) {
          TS_ASSERT_EQUALS(in[i], out[i]);
        }
      }
    }
    
    // integer boundary cases
    int64_t maxint = std::numeric_limits<int64_t>::max() >> 4;
    for (size_t multiplier = maxint; multiplier < maxint; ++multiplier) {
      size_t len = 128;
      uint64_t in[len];
      uint64_t out[len];
      for (size_t i = 0;i < len; ++i) {
        in[i] = (multiplier * i);
      }
      oarchive oarc;
      frame_of_reference_encode_128(in, len, oarc);

      iarchive iarc(oarc.buf, oarc.off);
      frame_of_reference_decode_128(iarc, len, out);
      TS_ASSERT_EQUALS(oarc.off, iarc.off);
      free(oarc.buf);

      for (size_t i = 0;i < len; ++i) {
        TS_ASSERT_EQUALS(in[i], out[i]);
      }
    }
  }
  void test_shift_encode() {
    int64_t maxint = std::numeric_limits<int64_t>::max();
    int64_t minint = std::numeric_limits<int64_t>::min();
    for (int64_t i = maxint - 256; i < maxint; ++i) {
      uint64_t j = shifted_integer_encode(i);
      int64_t i2 = shifted_integer_decode(j);
      TS_ASSERT_EQUALS(i, i2);
    }
    for (int64_t i = minint; i < minint + 256; ++i) {
      uint64_t j = shifted_integer_encode(i);
      int64_t i2 = shifted_integer_decode(j);
      TS_ASSERT_EQUALS(i, i2);
    }
    for (int64_t i = -256; i < 256; ++i) {
      uint64_t j = shifted_integer_encode(i);
      int64_t i2 = shifted_integer_decode(j);
      TS_ASSERT_EQUALS(i, i2);
    }
  }
};
