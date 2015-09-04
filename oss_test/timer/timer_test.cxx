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
/*  
 * Copyright (c) 2009 Carnegie Mellon University. 
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS
 *  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied.  See the License for the specific language
 *  governing permissions and limitations under the License.
 *
 * For more about this software visit:
 *
 *      http://www.graphlab.ml.cmu.edu
 *
 */


#include <timer/timer.hpp>
#include <unistd.h>
#include <cxxtest/TestSuite.h>
using namespace graphlab;

class timer_test: public CxxTest::TestSuite {
public:

  void test_timer() {
    timer ti;
    ti.start();
    double t = ti.current_time();
    TS_ASSERT_DELTA(t, 0, 0.1);
    sleep(3);
    t = ti.current_time();
    TS_ASSERT_DELTA(t, 3, 0.1);
  }


  void test_lowres_timer() {
    timer ti;
    int t = timer::approx_time_seconds();
    sleep(3);
    int t2 = timer::approx_time_seconds();
    TS_ASSERT_DELTA(t2 - t, 3, 1);
  }


};
