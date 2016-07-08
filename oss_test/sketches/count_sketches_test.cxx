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
#include <iostream>
#include <vector>
#include <cmath>
#include <map>
#include <unordered_map>
#include <sketches/countsketch.hpp>
#include <sketches/countmin.hpp>
#include <random/random.hpp>
#include <cxxtest/TestSuite.h>
#include <timer/timer.hpp>

class countsketch_test: public CxxTest::TestSuite {  
 private:

  /**
   * Create a set of random integers to be used to benchmark
   * the countsketch sketch. 
   * One can choose the number of unique values and the distribution
   * of each element's frequency. 
   * See the documentation for test_benchmark for more details.
   */
  std::vector<std::pair<size_t, size_t>> item_counts(size_t num_unique_items, 
      size_t count_per_item,
      bool exponential) {
 
    std::vector<std::pair<size_t, size_t>> v;
    float alpha = 1.0;  // parameter for exponential distribution

    for (size_t i = 0; i < num_unique_items; ++i) {
      size_t count = count_per_item;
      if (exponential) count = std::floor(count * graphlab::random::gamma(alpha));
      v.push_back(std::pair<size_t, size_t>(i, count));
    }

    std::sort(v.begin(), v.end(),
         [](const std::pair<int, int>& lhs, const std::pair<int, int>& rhs) {
            return lhs.second < rhs.second; } );
    return v;
  }
 
  /**
   * Run an experiment (described more fully in the documentation for test_benchmark).
   *
   * \param m a synthetic dataset
   * \param sketch a sketch object
   * \param num_to_compare the number of objects for which we want to compute RMSE
   * \param verbose 
   */
  template<typename T>
  std::map<std::string, double> run_experiment(std::vector<std::pair<size_t, size_t>> m, 
                                               T sketch,  
                                               size_t num_to_compare, 
                                               bool verbose) {

    std::map<std::string, double> result;
   
    // Compute sketch
    graphlab::timer ti;
    ti.start();
    for (auto kv : m) {
      sketch.add(kv.first, kv.second);
    }
    result["elapsed"] = ti.current_time();
    result["count"] = m.size();

    // Compute RMSE for least common items
    std::vector<size_t> items;
    std::vector<int64_t> estimated;
    std::vector<int64_t> actual;
    for (size_t i = 0; i < num_to_compare; ++i) { 
      size_t item = m[i].first;
      size_t true_value = m[i].second;
      int64_t estimate = sketch.estimate(item);

      items.push_back(item);
      estimated.push_back(estimate);
      actual.push_back((int64_t) true_value);
      if (verbose) 
        std::cout << item << " : " << true_value << " : " << estimate << std::endl; 
    }
    result["rmse_rare"] = rmse<int64_t>(estimated, actual);

    // Compute RMSE for most common items
    items.clear();
    estimated.clear();
    actual.clear();
    for (size_t i = m.size()-1; i > m.size() - num_to_compare; --i) { 
      size_t item = m[i].first;
      size_t true_value = m[i].second;
      int64_t estimate = sketch.estimate(item); 

      items.push_back(item);
      estimated.push_back(estimate);
      actual.push_back((int64_t) true_value);
      if (verbose) 
        std::cout << item << " : " << true_value << " : " << estimate << std::endl; 
 
    }
    result["rmse_common"] = rmse<int64_t>(estimated, actual); 

    // Compute the density of the sketch, i.e. the ratio of nonzeros
    result["density"] = sketch.density();

    if (verbose) sketch.print();

    return result;
  }

  // Helper function that computes RMSE of two vectors of objects.
  template <typename T>
  double rmse(std::vector<T> y, std::vector<T> yhat) {
    DASSERT_EQ(y.size(), yhat.size());
    double rmse = 0;
    for (size_t i = 0; i < y.size(); ++i) { 
      rmse += std::pow((double) y[i] - (double) yhat[i], 2.0);
    }
    return std::sqrt(rmse / (double) y.size());
  }

 public:

  /**
   * Small example to use for debugging.
   */
  void test_small_example() {

    size_t num_unique = 20;
    size_t mean_count_per_item = 5;
    bool expo = true;
    auto items = item_counts(num_unique, mean_count_per_item, expo);

    size_t num_bits = 4;
    size_t num_hash = 3;

    graphlab::sketches::countmin<size_t> cm(num_bits, num_hash);
    graphlab::sketches::countsketch<size_t> cs(num_bits, num_hash);

    for (auto kv : items) {
      std::cout << std::endl;
      for (size_t i = 0; i < kv.second; ++i) {
        cm.add(kv.first);
        cs.add(kv.first);
      }
      cm.print();
      cs.print();
    }

    for (auto kv : items) {
      std::cout << kv.first << ":" << kv.second << ":" << cm.estimate(kv.first) << std::endl;
    }
 
    for (auto kv : items) {
      std::cout << kv.first << ":" << kv.second << ":" << cs.estimate(kv.first) << std::endl;
    }
  } 


  /**
   * This benchmark compares the RMSE for predicting the frequency of objects in a stream when
   * for two sketches: the CountMin sketch and the CountSketch. 
   * The synthetic data set we create has a fixed number of objects (in this case simply integers)
   * and we create a stream where each object is observed a given number of times. We consider
   * the situation where the frequency is uniform across all items and where the frequency
   * has a geometric distribution (more or less); we keep the expected frequency per user fixed.
   *
   * Two metrics are chosen at this point: RMSE for the 20 most common items and RMSE for the 20
   * least common items. 
   *
   * We vary the width and depth of each sketch.
   *
   * The columns of the results table are:
   *   - type of sketch
   *   - number of hash functions (depth)
   *   - number of bits (2^b is the number of bins, i.e. width)
   *   - number of unique objects included in sketch
   *   - 0 if all objects appear with the same frequecy; 1 if exponentially distributed
   *   - RMSE of the observed vs. predicted frequency for the 20 most rare items
   *   - RMSE of the observed vs. predicted frequency for the 20 most common items
   *   - # updates / second (in millions)
   *   - "compression ratio": The size of the sketch / the number of unique elements
   *   - density of the sketch: proportion of nonzero elements in the counts matrix
   */
  void test_benchmark() {

    bool verbose = false;
    graphlab::random::seed(1002);

    // Set up synthetic data 
    size_t num_to_compare = 20;               // number of items to use when computing RMSE
    size_t num_unique = 100000;   // number of unique objects
    size_t mean_count_per_item = 15;          // expected number of observations per object

    // Set up experiment
    std::vector<size_t> num_hash{5, 10};      // number of hash functions to use for each sketch
    std::vector<size_t> bits{8, 10, 12, 14};  // number of bins to use for each sketch (2^bits)
     
    // Set up reporting
    std::cout.precision(5);
    std::cout << "\nsketch\t# hash\t# bits\t# uniq\texpon.\trmse_r\trmse_c\t"
              << "#items(M)/s\tratio\tdensity" << std::endl;

    // Consider both uniformly distributed and exponentially distributed per-object frequecies
    for (auto expo : {true, false}) {

      // Generate data
      auto items = item_counts(num_unique, mean_count_per_item, expo);

      for (auto h: num_hash) {
        for (auto b: bits) {

          std::vector<std::string> sketch_names = {"CountSketch", "CountMinSketch"};
          for (auto sk : sketch_names) {
            std::map<std::string, double> res;

            // Create sketch
            if (sk == "CountSketch") {
              graphlab::sketches::countsketch<size_t> cs(b, h);
              res = run_experiment(items, cs, num_to_compare, verbose);
            } else {
              graphlab::sketches::countmin<size_t> cm(b, h);
              res = run_experiment(items, cm, num_to_compare, verbose);
            }

            // Compute number of updates per second
            double rate = (double) res["count"] / res["elapsed"] / 1000000;

            // Compute "compression ratio": The size of the sketch / the number of unique elements
            double ratio = (double) (h * (1<<b)) / (double) num_unique; 

            std::cout << sk << "\t" << h << "\t" << b << "\t" << 
              num_unique << "\t" << expo << "\t" << 
              res["rmse_rare"] << "\t" << res["rmse_common"] << "\t" <<
              rate << "\t" << ratio << "\t" << res["density"] << std::endl;
          }
        }
      }
    }

    }
};
