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
#include <string>
#include <boost/iostreams/stream.hpp>
#include <fileio/cache_stream_sink.hpp>
#include <fileio/cache_stream_source.hpp>
#include <cxxtest/TestSuite.h>

using namespace graphlab::fileio;
using namespace graphlab::fileio_impl;


typedef boost::iostreams::stream<cache_stream_source> icache_stream;
typedef boost::iostreams::stream<cache_stream_sink> ocache_stream;

class cache_stream_test: public CxxTest::TestSuite {

 public:
   void test_read_write() {
     auto block = fixed_size_cache_manager::get_instance().new_cache("cache://0");

     std::string expected = "we require more minerals";
     ocache_stream out(block->get_cache_id());
     TS_ASSERT(out.good());
     out << expected;
     out.close();

     icache_stream in(block->get_cache_id());
     std::string value;
     TS_ASSERT(in.good());
     std::getline(in, value);
     TS_ASSERT(in.eof());
     in.close();
     TS_ASSERT_EQUALS(value, expected);
   }

   void test_read_write_large_blocks() {
     auto block = fixed_size_cache_manager::get_instance().new_cache("cache://1");

     ocache_stream out(block->get_cache_id());
     TS_ASSERT(out.good());

     const size_t BLOCK_SIZE = 1024; // 1K
     size_t NUM_BLOCKS = 1024; // 1K
     char buf[BLOCK_SIZE];
     for (size_t i = 0; i < NUM_BLOCKS; ++i) {
       memset(buf, (char)(i % 128), BLOCK_SIZE);
       out.write(buf, BLOCK_SIZE);
       TS_ASSERT(out.good());
     }
     out.close();

     icache_stream in(block->get_cache_id());
     TS_ASSERT(in.good());

     for (size_t i = 0; i < NUM_BLOCKS; ++i) {
       in.read(buf, BLOCK_SIZE);
       TS_ASSERT(in.good());
       for (size_t j = 0; j < BLOCK_SIZE; ++j) {
         TS_ASSERT_EQUALS(buf[j], (char)(i % 128));
       }
     }
     in.read(buf, 1);
     TS_ASSERT(in.eof());
     in.close();
   }

   void test_seek() {
     graphlab::fileio::FILEIO_MAXIMUM_CACHE_CAPACITY_PER_FILE = 1024*1024;
     const size_t block_size = graphlab::fileio::FILEIO_MAXIMUM_CACHE_CAPACITY_PER_FILE;
     _test_seek_helper(block_size / 2);
     _test_seek_helper(block_size);
     _test_seek_helper(block_size * 2);
   }

   /**
    * Write file_size bytes of data to the cache_stream, and test
    * read with random seek.
    */
   void _test_seek_helper(size_t file_size) {
     auto block = fixed_size_cache_manager::get_instance().new_cache("cache://2");

     ocache_stream out(block->get_cache_id());
     for (size_t i = 0; i < (file_size / sizeof(size_t)); ++i) {
       out.write(reinterpret_cast<char*>(&i), sizeof(size_t));
     }
     TS_ASSERT(out.good());
     out.close();

     icache_stream in(block->get_cache_id());
     for (size_t i = 0; i < (file_size / sizeof(size_t)); ++i) {
      size_t j = (i * 17) % (file_size / sizeof(size_t));
      in.seekg(j * sizeof(size_t), std::ios_base::beg);
      size_t v;
      in.read(reinterpret_cast<char*>(&v), sizeof(size_t));
      ASSERT_EQ(v, j);
    }
   }
};
