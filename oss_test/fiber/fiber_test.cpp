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
#include <iostream>
#include <fiber/fiber_group.hpp>
#include <timer/timer.hpp>
using namespace graphlab;
int numticks = 0;
void threadfn() {

  timer ti; ti.start();
  while(1) {
    if (ti.current_time() >= 1) break;
    fiber_control::yield();
    __sync_fetch_and_add(&numticks, 1);
  }
}


void threadfn2() {

  timer ti; ti.start();
  while(1) {
    if (ti.current_time() >= 2) break;
    fiber_control::yield();
    __sync_fetch_and_add(&numticks, 2);
  }
}

int main(int argc, char** argv) {
  timer ti; ti.start();
  fiber_group group;
  fiber_group group2;
  for (int i = 0;i < 100000; ++i) {
    group.launch(threadfn);
    group2.launch(threadfn2);
  }
  group.join();
  std::cout << "Completion in " << ti.current_time() << "s\n";
  std::cout << "Context Switches: " << numticks << "\n";
  group2.join();
  std::cout << "Completion in " << ti.current_time() << "s\n";
  std::cout << "Context Switches: " << numticks << "\n";
}
