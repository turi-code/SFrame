/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <iostream>
#include <vector>
#include <cassert>
#include <graph/graph/dcsc_store.hpp>
#include <graphlab/macros_def.hpp>
using namespace graphlab;

int main(int argc, char** argv) {
  dcsc_store<uint32_t> store;
  std::cout << store;
  // basic tests
  store.insert(1, 2, 1);
  store.insert(2, 5, 2);
  store.insert(4, 4, 3);
  store.insert(4, 5, 4);
  store.insert(0, 1, 5);
  store.insert(0, 5, 6);
  store.insert(0, 3, 7);
  store.insert(3, 3, 8);
  store.insert(4, 3, 9);

  std::cout << store;

  std::cout << "\n\nPrinting column 0\n";
  typedef dcsc_store<uint32_t>::entry_type entry_type;
  foreach(const entry_type e, store.get_column(0)) {
    std::cout << "(" << e.row() << ", " << e.column() << ") = " << e.value() << "\n";
  }

  std::cout << "\n\nPrinting column 5\n";
  foreach(entry_type e, store.get_column(5)) {
    std::cout << "(" << e.row() << ", " << e.column() << ") = " << e.value() << "\n";
  }


  std::cout << "\n\nChanging column 3 to all 1s\n";
  foreach(entry_type e, store.get_column(3)) {
    e.value() = 1;
  }

  std::cout << store;

  srand(10);
  store.clear();
  std::vector<uint32_t> row, col, val;
  for (size_t i = 0;i < 10000; ++i) {
    row.push_back(rand());
    col.push_back(rand());
    val.push_back(rand());
  }

  store.construct(row.begin(), row.end(),
                  col.begin(), col.end(),
                  val.begin(), val.end());

  for (size_t i = 0;i < 10000; ++i) {
    assert(store.find(row[i], col[i]) == val[i]);
  }


}
