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
#include <rpc/dc.hpp>
#include <rpc/sample_sort.hpp>
#include <algorithm>

using namespace graphlab;

int main(int argc, char** argv) {
  mpi_tools::init(argc, argv);
  distributed_control dc;
  std::vector<size_t> keys;
  std::vector<size_t> values;
  for (size_t i = 0;i < 1000000; ++i) {
    size_t s = rand();
    keys.push_back(s); values.push_back(s);
  }

  sample_sort<size_t, size_t> sorter(dc);
  sorter.sort(keys.begin(), keys.end(),
              values.begin(), values.end());

  std::vector<std::vector<std::pair<size_t, size_t> > > result(dc.numprocs());
  
  std::swap(result[dc.procid()], sorter.result());
  dc.gather(result, 0);
  if (dc.procid() == 0) {
    // test that it is sorted and the values are correct
    size_t last = 0;
    for (size_t i = 0;i < result.size(); ++i) {
      dc.cout() << result[i].size() << ",";
      for (size_t j = 0; j < result[i].size(); ++j) {
        ASSERT_EQ(result[i][j].first, result[i][j].second);
        ASSERT_GE(result[i][j].first, last);
        last = result[i][j].first;
      }
    }
    dc.cout() << std::endl;
  }
  mpi_tools::finalize();
}
