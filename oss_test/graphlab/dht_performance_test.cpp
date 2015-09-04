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


#include <iostream>
#include <graphlab/util/timer.hpp>
#include <rpc/mpi_tools.hpp>
#include <graphlab/util/generics/any.hpp>
#include <graphlab/rpc/dc.hpp>
#include <graphlab/rpc/dc_init_from_mpi.hpp>    
#include <graphlab/rpc/dht.hpp>
#include <logger/logger.hpp>
using namespace graphlab;

std::string randstring(size_t len) {
  std::string str;
  str.resize(len);
  const char *charset="ab";
  size_t charsetlen = 64;
  for (size_t i = 0;i < len; ++i) {
    str[i] = charset[rand()  % charsetlen];
  }
  return str;
}

int main(int argc, char ** argv) {
  //mpi_tools::init(argc, argv);
  global_logger().set_log_level(LOG_INFO);

  dc_init_param param;
  mpi_tools::init(argc, argv);
  if (!init_param_from_mpi(param)) {
    return 0;
  }
  
  global_logger().set_log_level(LOG_DEBUG);
  distributed_control dc(param);
  std::cout << "I am machine id " << dc.procid() 
            << " in " << dc.numprocs() << " machines"<<std::endl;
  dht<std::string, std::string> testdht(dc);
  
  std::vector<std::pair<std::string, std::string> > data;
  const size_t NUMSTRINGS = 10000;
  const size_t strlen[4] = {16, 128, 1024, 10240};
  // fill rate
  for (size_t l = 0; l < 4; ++l) {
    timer ti;
    ti.start();
    if (dc.procid() == 0) {
      std::cout << "String Length = " << strlen[l] << std::endl;
      data.clear();
      for (size_t i = 0;i < NUMSTRINGS; ++i) {
        data.push_back(std::make_pair(randstring(8), randstring(strlen[l])));
      }
      std::cout << "10k random strings generated" << std::endl;
      std::cout << "Starting set" << std::endl;
      for (size_t i = 0;i < NUMSTRINGS; ++i) {
        testdht.set(data[i].first, data[i].second);
        if (i % 100 == 0) {
          std::cout << ".";
          std::cout.flush();
        }
      }
      std::cout << "10k insertions in " << ti.current_time() << std::endl;
    }
      dc.full_barrier();
    if (dc.procid() == 0) {
      std::cout << "--> Time to Insertion Barrier " << ti.current_time() << std::endl;
    }
    // get rate
    if (dc.procid() == 0) {
      std::cout << "Starting get" << std::endl;

      timer ti;
      ti.start();
      for (size_t i = 0;i < NUMSTRINGS; ++i) {
        std::pair<bool, std::string> ret = testdht.get(data[i].first);
        assert(ret.first);
        if (i % 100 == 0) {
          std::cout << ".";
          std::cout.flush();
        }
      }
      std::cout << "10k reads in " << ti.current_time() << std::endl;
    }
    testdht.clear();
  }
  dc.barrier();
  testdht.print_stats();
  mpi_tools::finalize();
}
