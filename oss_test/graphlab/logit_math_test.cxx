/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <string>
#include <cmath>
#include <graphlab/util/logit_math.hpp>
#include <logger/logger.hpp>
#include <logger/assertions.hpp>

using namespace graphlab;
using namespace std;

class logit_math : public CxxTest::TestSuite {
 public:
  template <typename FTrue, typename FTest>
  void _run_test(std::string name, FTrue f_true, FTest f_test, double abs_range, double numerical_accuracy) {

    for(int sign : {-1, 1} ) {

      double r = abs_range;

      while(r > numerical_accuracy) {

        double x_true = f_true(sign * r);
        double x_test = f_test(sign * r);

        if((std::abs(x_true - x_test)
            / (numerical_accuracy + std::abs(x_true) + std::abs(x_test))
            > numerical_accuracy)) {

          std::string msg = (
              name + ": For r = "
              + std::to_string(r)
              + ": (true) " + std::to_string(x_true) + " != " + std::to_string(x_test) + " (test)");

          ASSERT_MSG(false, msg.c_str());
        }

        r /= 2;
      }
    }
  }

  void test_log1pe() {
    _run_test("log1pe",
              [](double x) { return std::log1p(std::exp(x)); },
              [](double x) { return log1pe(x); },
              10,
              1e-8);
  }

  void test_log1pen() {
    _run_test("log1pen",
              [](double x) { return std::log1p(std::exp(-x)); },
              [](double x) { return log1pen(x); },
              10,
              1e-8);
  }

  void test_logem1() {
    _run_test("logem1",
              [](double x) { return std::log(std::exp(x) - 1); }, 
              [](double x) { return logem1(x); },
              10,
              1e-8);
  }
  
  void test_log1pe_derivative() {
    _run_test("log1pe_deriviative",
              [](double x) { return 1.0 / (1.0 + std::exp(-x)); },
              [](double x) { return log1pe_deriviative(x); },
              10,
              1e-8);
  }

  void test_log1pen_derivative() {
    _run_test("log1pen_deriviative",
              [](double x) { return -1.0 / (1.0 + std::exp(x)); },
              [](double x) { return log1pen_deriviative(x); },
              10,
              1e-8);
  }


  void test_sigmoid() {
    _run_test("sigmoid",
              [](double x) { return 1.0 / (1.0 + std::exp(-x)); },
              [](double x) { return sigmoid(x); },
              10,
              1e-8);
  }

};
