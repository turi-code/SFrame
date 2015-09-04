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
#include <sframe/sarray_file_format_v1.hpp>
#include <timer/timer.hpp>
#include <random/random.hpp>

using namespace graphlab;

class sarray_file_format_v1_test: public CxxTest::TestSuite {
 public:
  void test_file_format_v1_basic(void) {
    // write a file
    sarray_format_writer_v1<size_t> writer;
    
    // before open, all segment operations will fail
    TS_ASSERT_THROWS_ANYTHING(writer.num_segments());
    TS_ASSERT_THROWS_ANYTHING(writer.open_segment(5));

#ifndef NDEBUG
    TS_ASSERT_THROWS_ANYTHING(writer.write_segment(3, 1));
#endif

    // open with 4 segments
    std::string test_file_name = get_temp_name() + ".sidx";
    writer.open(test_file_name, 4);
    TS_ASSERT_EQUALS(writer.num_segments(), 4);
    // open all segments
    for (size_t i = 0;i < 4; ++i) writer.open_segment(i);
    // no segment 4
    TS_ASSERT_THROWS_ANYTHING(writer.open_segment(4));
    for (size_t i = 0;i < 4; ++i) {
      for (size_t j = 0;j < 100; ++j) {
        writer.write_segment(i, j);
      }
    }
    // no segment 4 to write to
#ifndef NDEBUG
    TS_ASSERT_THROWS_ANYTHING(writer.write_segment(4, 2));
#endif
    for (size_t i = 0;i < 4; ++i) {
      writer.close_segment(i);
      // cannot write after close
#ifndef NDEBUG
      TS_ASSERT_THROWS_ANYTHING(writer.write_segment(i, 0));
#endif
    }
    writer.close();

    // now see if I can read it
    sarray_format_reader_v1<size_t> reader;
    std::vector<size_t> bad_result;
#ifndef NDEBUG
    TS_ASSERT_THROWS_ANYTHING(reader.read_rows(1, 2, bad_result));
#endif

    reader.open(test_file_name);
    index_file_information info = reader.get_index_info();
    // check the meta data
    TS_ASSERT_EQUALS(info.version, 1);
    TS_ASSERT_EQUALS(info.content_type, typeid(size_t).name());
    // check segments and segment sizes
    TS_ASSERT_EQUALS(info.nsegments, 4);
    for (size_t i = 0; i < 4; ++i) {
      TS_ASSERT_EQUALS(info.segment_sizes[i], 100);
      TS_ASSERT_EQUALS(reader.segment_size(i), 100);
    }
    TS_ASSERT_EQUALS(info.segment_sizes.size(), 4);
    // read the data we wrote the last time
    for (size_t i = 0; i < 4; ++i) {
      for (size_t j = 0;j < 100; ++j) {
        std::vector<size_t> val;
        reader.read_rows(i*100 + j, i*100 + j + 1, val);
        TS_ASSERT_EQUALS(val.size(), 1);
        TS_ASSERT_EQUALS(val[0], j);
      }
    }
    // reading from nonexistant segment is an error

    reader.close();
  }

  static const size_t LARGE_SIZE = 1024*1024;
  void test_file_format_v1_large(void) {
    // write a file
    sarray_format_writer_v1<size_t> writer;

    // open with 4 segments
    std::string test_file_name = get_temp_name() + ".sidx";
    writer.open(test_file_name, 4);
    TS_ASSERT_EQUALS(writer.num_segments(), 4);
    // open all segments
    for (size_t i = 0;i < 4; ++i) writer.open_segment(i);
    for (size_t i = 0;i < 4; ++i) {
      for (size_t j = 0;j < LARGE_SIZE; ++j) {
        writer.write_segment(i, j);
      }
    }
    writer.close();
    {
      // now see if I can read it
      sarray_format_reader_v1<size_t> reader;
      size_t bad_result;

      reader.open(test_file_name);
      index_file_information info = reader.get_index_info();
      // check the meta data
      TS_ASSERT_EQUALS(info.version, 1);
      TS_ASSERT_EQUALS(info.content_type, typeid(size_t).name());
      // check segments and segment sizes
      TS_ASSERT_EQUALS(info.nsegments, 4);
      for (size_t i = 0; i < 4; ++i) {
        TS_ASSERT_EQUALS(info.segment_sizes[i], LARGE_SIZE);
        TS_ASSERT_EQUALS(reader.segment_size(i), LARGE_SIZE);
      }
      TS_ASSERT_EQUALS(info.segment_sizes.size(), 4);

      // read the data we wrote the last time
      for (size_t i = 0; i < 4; ++i) {
        for (size_t j = 0;j < LARGE_SIZE; ++j) {
          std::vector<size_t> val;
          reader.read_rows(i*LARGE_SIZE+ j, i*LARGE_SIZE+ j + 1, val);
          TS_ASSERT_EQUALS(val.size(), 1);
          TS_ASSERT_EQUALS(val[0], j);
        }
      }

      reader.close();
    }
  }




  static const size_t VERY_LARGE_SIZE = 4*1024*1024;
  void test_random_access(void) {
    // write a file
    sarray_format_writer_v1<size_t> writer;

    timer ti;
    ti.start();
    // open with 4 segments
    std::string test_file_name = get_temp_name() + ".sidx";
    writer.open(test_file_name, 16);
    TS_ASSERT_EQUALS(writer.num_segments(), 16);
    // open all segments, write one sequential value spanning all segments
    for (size_t i = 0;i < 16; ++i) writer.open_segment(i);
    size_t v = 0;
    for (size_t i = 0;i < 16; ++i) {
      for (size_t j = 0;j < VERY_LARGE_SIZE; ++j) {
        writer.write_segment(i, v);
        ++v;
      }
    }
    writer.close();
    std::cout << "Written 16*4M = 64M integers to disk sequentially in: " 
              << ti.current_time() << " seconds \n";


    // random reads
    {
      ti.start();
      // now see if I can read it
      sarray_format_reader_v1<size_t> reader;
      reader.open(test_file_name);
      random::seed(10001);
      for (size_t i = 0;i < 1600; ++i) {
        size_t len = 4096;
        size_t start = random::fast_uniform<size_t>(0, 16 * VERY_LARGE_SIZE - 4097);
        std::vector<size_t> vals;
        reader.read_rows(start, start + len, vals);
        TS_ASSERT_EQUALS(vals.size(), len);
        for (size_t i = 0; i < len; ++i) {
          TS_ASSERT_EQUALS(vals[i], start + i);
        }
      }
      std::cout << "1600 random seeks of 4096 values in " 
                << ti.current_time() << "seconds\n" << std::endl;

      // test some edge cases. Read past the end
      size_t end = 16 * VERY_LARGE_SIZE;
      std::vector<size_t> vals;
      size_t ret = reader.read_rows(end - 5, 2 * end, vals);
      TS_ASSERT_EQUALS(ret, 5);
      TS_ASSERT_EQUALS(vals.size(), 5);
      for (size_t i = 0;i < vals.size(); ++i) {
        TS_ASSERT_EQUALS(vals[i], end - (5 - i));
      }
    }

    // random sequential reads
    {
      ti.start();
      // now see if I can read it
      sarray_format_reader_v1<size_t> reader;
      reader.open(test_file_name);
      random::seed(10001);
      std::vector<size_t> start_points;
      for (size_t i = 0;i < 16; ++i) {
        start_points.push_back(
            random::fast_uniform<size_t>(0, 
                                         15 * VERY_LARGE_SIZE));
                                         // 15 so as to give some gap for reading
      }
      for (size_t i = 0;i < 100; ++i) {
        for (size_t j = 0;j < start_points.size(); ++j) {
          size_t len = 4096;
          std::vector<size_t> vals;
          reader.read_rows(start_points[j], start_points[j] + len, vals);
          TS_ASSERT_EQUALS(vals.size(), len);
          for (size_t k = 0; k < len; ++k) {
            TS_ASSERT_EQUALS(vals[k], start_points[j] + k);
          }
          start_points[j] += len;
        }
      }
      std::cout << "1600 semi-sequential seeks of average 4096 values in " 
                << ti.current_time() << "seconds\n" << std::endl;
    }
  }

};
