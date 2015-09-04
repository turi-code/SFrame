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
#include <vector>
#include <cmath>

#include <iostream>
#include <logger/logger.hpp>
#include <logger/assertions.hpp>
#include <graphlab/util/bitops.hpp>
#include <util/cityhash_gl.hpp>
#include <util/fast_integer_power.hpp>

using namespace graphlab;
using namespace std;

class fast_power_test : public CxxTest::TestSuite {
 public:
  void _run_test(double v, const std::vector<size_t>& powers) {

    fast_integer_power vp(v);

    for(size_t n : powers) {
      double v_ref = std::pow(v, n);
      double v_check = vp.pow(n);

      if(abs(v_ref - v_check) / (1.0 + ceil(v_ref + v_check) ) > 1e-8 ) {
        std::ostringstream ss;
        ss << "Wrong value: "
           << v << " ^ " << n << " = " << v_ref
           << "; retrieved = " << v_check << std::endl;

        ASSERT_MSG(false, ss.str().c_str());
      }
    }
  }

  void test_low_powers() {
    _run_test(0.75, {0, 1, 2, 3, 4, 5, 6, 7, 8});
  }

  void test_lots_of_powers() {
    std::vector<size_t> v(5000);

    for(size_t i = 0; i < 5000; ++i)
      v[i] = i;

    _run_test(0.99, v);
    _run_test(1.02, v);
  }

  void test_many_random() {
    std::vector<size_t> v(50000);

    for(size_t i = 0; i < 50000; ++i)
      v[i] = hash64(i);

    _run_test(1 - 1e-6, v);
    _run_test(1 + 1e-6, v);
  }
};
