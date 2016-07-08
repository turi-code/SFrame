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
#include <fileio/temp_files.hpp>
#include <cxxtest/TestSuite.h>

using namespace graphlab;


class temp_file_test : public CxxTest::TestSuite {

 public:

  void test_temp_file() {
    // give me 3 temp names
    std::string filea = get_temp_name();
    std::string fileb = get_temp_name();
    std::string filec = get_temp_name();

    // create file a. it will just be the file itself
    { 
      std::ofstream fout(filea.c_str()); fout.close();
      TS_ASSERT(delete_temp_file(filea));
      // check that filea is gone
      std::ifstream fin(filea.c_str());
      TS_ASSERT(fin.fail());
      // repeated deletion fails
      TS_ASSERT(delete_temp_file(filea) == false);
    }


    // create file b. it will have on prefix
    {
      fileb = fileb + ".cogito";
      std::ofstream fout(fileb.c_str()); fout.close();
      delete_temp_file(fileb);
      // check that fileb is gone
      std::ifstream fin(fileb.c_str());
      TS_ASSERT(fin.fail());
      // repeated deletion fails
      TS_ASSERT(delete_temp_file(fileb) == false);
    } 

    // file c is a lot of prefixes. and tests that we can delete a bunch of 
    // stuff
    {
      std::vector<std::string> filecnames{filec + "pika",
        filec + ".chickpeas",
        filec + ".gyro",
        filec + ".salamander"};
      for(std::string f : filecnames) {
        std::ofstream fout(f.c_str()); 
        fout.close();
      }
      delete_temp_files(filecnames);
      // check that they are all gone
      for(std::string f : filecnames) {
        std::ifstream fin(f.c_str()); 
        TS_ASSERT(fin.fail());
        // repeated deletion fails
        TS_ASSERT(delete_temp_file(f) == false);
      }
    }
  }
};

