/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <cmath>
#include <lambda/python_api.hpp>
#include <lambda/python_thread_guard.hpp>
#include <timer/timer.hpp>
#include <lambda/pyflexible_type.hpp>
#include <boost/python.hpp>
#include <cstdlib>

using namespace graphlab;
using namespace graphlab::lambda;

const int N = 10000; // number of elements
const int n = 1000; // (string) length of each element

std::string random_str(size_t len) {
  static const char alphanum[] =
      "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";
  std::string s;
  for (size_t i = 0; i < len; ++i) {
    s.push_back(alphanum[rand() % (sizeof(alphanum) - 1)]);
  }
  return s;
}

void test_string() {
  std::vector<std::string> string_list(N);
  std::vector<flexible_type> flex_string_list(N);
  std::vector<python::object> obj_list(N);

  for (size_t i = 0; i < N; ++i) {
    auto s = random_str(n);
    flex_string_list[i] = s;
    string_list[i] = s;
  }

  timer mytimer;
  // flex str to obj
  mytimer.start();
  for (int i = 0; i < N; ++i) {
    obj_list[i] = PyObject_FromFlex(flex_string_list[i]);
  }
  double time_fstring_to_obj = mytimer.current_time();

  // str to obj
  mytimer.start();
  for (int i = 0; i < N; ++i) {
    obj_list[i] = python::object(python::handle<>(
                    PyString_FromString(string_list[i].c_str())));
  }
  double time_string_to_obj = mytimer.current_time();

  std::cout << "String to PyObject test:\n"
            << "convert " << N << " flex string to object takes "
            << time_fstring_to_obj << " secs\n"
            << "convert " << N << " string to object takes "
            << time_string_to_obj << " secs\n"
            << "overhead: " << (time_fstring_to_obj / time_string_to_obj)
            << "\n" << std::endl;


  // flex str from obj
  mytimer.start();
  for (int i = 0; i < N; ++i) {
    // flex_string_list[i] = PyObject_AsFlex(obj_list[i]);
    PyObject_AsFlex(obj_list[i], flex_string_list[i]);
  }
  double time_obj_to_fstring = mytimer.current_time();

  // str from obj
  mytimer.start();
  for (int i = 0; i < N; ++i) {
    string_list[i] = python::extract<std::string>(obj_list[i]);
  }
  double time_obj_to_string = mytimer.current_time();

  std::cout << "PyObject to String test:\n"
            << "convert " << N << " object to flex string takes "
            << time_obj_to_fstring << " secs\n"
            << "convert " << N << " object to string takes "
            << time_obj_to_string << " secs\n"
            << "overhead: " << (time_obj_to_fstring / time_obj_to_string)
            << "\n" << std::endl;
}

void test_int() {
  std::vector<size_t> int_list(N);
  std::vector<flexible_type> flex_int_list(N);
  std::vector<python::object> obj_list(N);

  for (size_t i = 0; i < N; ++i) {
    int x = rand();
    int_list[i] = x;
    flex_int_list[i] = x;
  }

  timer mytimer;
  
  // flex int to obj
  mytimer.start();
  for (int i = 0; i < N; ++i) {
    obj_list[i] = PyObject_FromFlex(flex_int_list[i]);
  }
  double time_fint_to_obj = mytimer.current_time();

  // int to obj
  mytimer.start();
  for (int i = 0; i < N; ++i) {
    obj_list[i] = python::object(python::handle<>(
                    PyInt_FromLong(int_list[i])));
  }
  double time_int_to_obj = mytimer.current_time();
 
  std::cout << "Int to PyObject test:\n"
            << "convert " << N << " flex int to object takes "
            << time_fint_to_obj << " secs\n"
            << "convert " << N << " int to object takes "
            << time_int_to_obj << " secs\n"
            << "overhead: " << (time_fint_to_obj / time_int_to_obj)
            << "\n" << std::endl;

  // flex int from obj
  mytimer.start();
  for (int i = 0; i < N; ++i) {
    flex_int_list[i] = PyObject_AsFlex(obj_list[i]);
  }
  double time_obj_to_fint = mytimer.current_time();

  // int from obj
  mytimer.start();
  for (int i = 0; i < N; ++i) {
    int_list[i] = python::extract<flex_int>(obj_list[i]);
  }
  double time_obj_to_int = mytimer.current_time();

  std::cout << "PyObject to int test:\n"
            << "convert " << N << " object to flex int takes "
            << time_obj_to_fint << " secs\n"
            << "convert " << N << " object to int takes "
            << time_obj_to_int << " secs\n"
            << "overhead: " << (time_obj_to_fint / time_obj_to_int)
            << "\n" << std::endl;
}


int main(int argc, char** argv) {
  lambda::init_python(argc, argv);
  {
    python_thread_guard py_thread_gurad; // GIL
    test_int();
    test_string();
  }
  return 0;
}
