/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <string>
#include <random>
#include <set>
#include <sstream>
#include <vector>
#include <array>
#include <algorithm>
#include <util/cityhash_gl.hpp>
#include <cxxtest/TestSuite.h>

// SFrame and Flex type
#include <table_printer/table_printer.hpp>
#include <sframe/testing_utils.hpp>

using namespace graphlab;

class test_table_printer : public CxxTest::TestSuite {

 public:

  void test_table() {
    size_t num_columns = 4;

    sframe sf = make_random_sframe(100, "cnsb");

    std::vector<std::vector<flexible_type> > values = testing_extract_sframe_data(sf);

    std::vector<std::pair<std::string, size_t> > format(num_columns);

    for(size_t i = 0; i < num_columns; ++i) {
      format[i].first = sf.column_name(i);
      format[i].second = 0;
    }

    for(size_t interval : {1, 5} ) {

      table_printer table({{"Tick", 0}, {"Time", 0}, {"C1", 0}, {"C2", 0}, {"S1", 0}, {"B1", 2}}, interval);

      for(size_t i = 0; i < values.size(); ++i) {
        table.print_progress_row(i, i, progress_time(),
                                 long(values[i][0]), double(values[i][1]),
                                 std::string(values[i][2]), bool(values[i][3]));
      }

      sframe saved_sf = table.get_tracked_table();

      std::vector<std::vector<flexible_type> > saved_values = testing_extract_sframe_data(saved_sf);

      DASSERT_EQ(saved_values.size(), size_t(100) / interval);

      for(size_t i = 0; i < saved_values.size(); ++i) {

        size_t table_idx = i * interval;

        DASSERT_TRUE(saved_values[i][0].get_type() == flex_type_enum::INTEGER);
        DASSERT_TRUE(saved_values[i][0] == table_idx);
        DASSERT_TRUE(saved_values[i][1].get_type() == flex_type_enum::FLOAT);
        DASSERT_TRUE(saved_values[i][2] == values[table_idx][0]);
        DASSERT_TRUE(saved_values[i][3] == values[table_idx][1]);
        DASSERT_TRUE(saved_values[i][4] == values[table_idx][2]);
        DASSERT_TRUE(saved_values[i][5] == values[table_idx][3]);
      }
    }
  }
};
