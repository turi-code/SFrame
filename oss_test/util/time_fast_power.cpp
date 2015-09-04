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
#include <string>
#include <timer/timer.hpp>
#include <util/fast_integer_power.hpp>
#include <util/cityhash_gl.hpp>
#include <random/random.hpp>

using namespace graphlab;

static constexpr size_t n_iterations = 10000000;

void _run_time_test(size_t max_value) GL_HOT_INLINE_FLATTEN;

void _run_time_test(size_t max_value) {

  double v = std::pow(0.000001, 1.0 / max_value);

  std::vector<size_t> powers(n_iterations);

  for(size_t i = 0; i < n_iterations; ++i)
    powers[i] = random::fast_uniform<size_t>(0, max_value);


  {
    timer tt;
    tt.start();

    double x = 0;
    for(size_t i = 0; i < n_iterations; ++i) {
      x += std::pow(v, powers[i]);
    }

    std::cout << "  Time with std power function (" << n_iterations << " iterations, x = " << x << "): "
              << tt.current_time() << 's' << std::endl;
  }

  {
    timer tt;
    tt.start();

    fast_integer_power fip(v);

    double x = 0;
    size_t n_times = 20;
    for(size_t iter = 0; iter < n_times; ++iter) {
      for(size_t i = 0; i < n_iterations; ++i) {
        x += fip.pow(powers[i]);
      }
    }

    std::cout << "  Time with new power function (" << n_iterations << " iterations, x = " << x / n_times << "): "
              << tt.current_time() / n_times << 's' << std::endl;
  }

}

int main(int argc, char **argv) {

  std::cout << "Small integers (0 - 65535): " << std::endl;
  _run_time_test( (1UL << 16) );

  std::cout << "Medium integers (0 - 2^32): " << std::endl;

  _run_time_test( (1UL << 32) );

  std::cout << "Large integers (0 - 2^48): " << std::endl;

  _run_time_test( (1UL << 48) );
}
