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
#include <fileio/general_fstream.hpp>
#include <logger/logger.hpp>
#include <logger/assertions.hpp>

/**
 * Test the general_fstream.
 * This program will read from the file_url, writes "hello world" to the stream,
 * read from it and check contents are equal.
 */
int main(int argc, char** argv) {
  if (argc != 2) {
    std::cerr << "Usage: ./general_fstream_test file_url\nExamples:\n"
              << "./general_fstream_test /tmp/foo.txt\n"
              << "./general_fstream_test hdfs:///tmp/foo.txt\n"
              << "./general_fstream_test s3://[access_key_id]:[secret_key]:[bucket]/bar.txt\n"
              << "./general_fstream_test hdfs://[host]:[port]/path\n";
    return 0;
  }
  std::string url(argv[1]);
  global_logger().set_log_level(LOG_INFO);
  std::string s;
  s.resize(16);
  for (size_t i = 0;i < 8; ++i) {
    s[2 * i] = 255;
    s[2 * i + 1] = 'a';
  }
  std::string expected;
  std::string buffer;
  try {
    std::cout << "Write to: " << url << std::endl;
    graphlab::general_ofstream fout(url);
    for (size_t i = 0;i < 4096; ++i) {
      fout.write(s.c_str(), s.length());
      expected = expected + s;
    }
    ASSERT_TRUE(fout.good());
    fout.close();

    std::cout << "Read from: " << url << std::endl;
    graphlab::general_ifstream fin(url);
    getline(fin, buffer);
    ASSERT_EQ(buffer, expected);
    fin.close();
  } catch(std::string& e) {
    std::cerr << "Exception: " << e << std::endl;
  }
  if (buffer != expected) return 1;


  // test seeks
  std::cout << "Rewriting for seek test: " << url << std::endl;
  {
    graphlab::general_ofstream fout(url);
    for (size_t i = 0;i < 4096; ++i) {
      // write a 4K block
      fout.write(reinterpret_cast<char*>(&i), sizeof(size_t));
      char c[4096] = {0};
      fout.write(c, 4096 - sizeof(size_t));
    }
    ASSERT_TRUE(fout.good());
    fout.close();
  }
  std::cout << "Seeking everywhere in : " << url << std::endl;
  {
    graphlab::general_ifstream fin(url);
    for (size_t i = 0; i< 4096; ++i) {
      size_t j = (i * 17) % 4096;
      fin.seekg(4096 * j, std::ios_base::beg);
      size_t v;
      fin.read(reinterpret_cast<char*>(&v), sizeof(size_t));
      ASSERT_EQ(v, j);
    }
  }
  return 0;
}
