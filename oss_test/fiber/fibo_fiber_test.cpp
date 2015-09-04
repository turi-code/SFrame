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
#include <boost/bind.hpp>
#include <fiber/fiber_control.hpp>
using namespace graphlab;

struct fibonacci_compute_promise {
  mutex* lock;
  size_t argument;
  size_t result;
  size_t parent_tid;
  bool result_set;
};

void fibonacci(fibonacci_compute_promise* promise) {
  //std::cout << promise->argument << "\n";
  if (promise->argument == 1 ||  promise->argument == 2) {
    promise->result = 1;
  } else {
    // recursive case
    mutex lock;
    fibonacci_compute_promise left, right;
    left.lock = &lock;
    left.argument = promise->argument - 1;
    left.result_set = false;
    left.parent_tid = fiber_control::get_tid();

    right.lock = &lock;
    right.argument = promise->argument - 2;
    right.result_set = false;
    right.parent_tid = fiber_control::get_tid();

    fiber_control::get_instance().launch(boost::bind(fibonacci, &left));
    fiber_control::get_instance().launch(boost::bind(fibonacci, &right));

    // wait on the left and right promise
    lock.lock();
    while (left.result_set == false || right.result_set == false) {
      fiber_control::deschedule_self(&lock.m_mut);
      lock.lock();
    }
    lock.unlock();

    assert(left.result_set);
    assert(right.result_set);
    promise->result = left.result + right.result;
  }
  promise->lock->lock();
  promise->result_set = true;
  if (promise->parent_tid) fiber_control::schedule_tid(promise->parent_tid);
  promise->lock->unlock();
}


int main(int argc, char** argv) {

  timer ti; ti.start();

  fibonacci_compute_promise promise;
  mutex lock;
  promise.lock = &lock;
  promise.result_set = false;
  promise.argument = 24;
  promise.parent_tid = 0;
  fiber_control::get_instance().launch(boost::bind(fibonacci, &promise));
  fiber_control::get_instance().join();
  assert(promise.result_set);
  std::cout << "Fib(" << promise.argument << ") = " << promise.result << "\n";

  std::cout << "Completion in " << ti.current_time() << "s\n";
  std::cout << fiber_control::get_instance().total_threads_created() << " threads created\n";
}
