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
#include <graphlab/util/generics/test_function_or_functor_type.hpp>

struct ts{
  int i;
};


void by_value(ts) {
}

void by_const_value(const ts) {
}

void by_reference(ts&) {
}

void by_const_reference(const ts&) {
}


struct functor_by_value{
  void operator()(ts) { }
};

struct functor_by_const_value{
  void operator()(const ts) { }
};

struct functor_by_reference{
  void operator()(ts&) { }
};

struct functor_by_const_reference{
  void operator()(const ts&) { }
};


struct const_functor_by_value{
  void operator()(ts) const { }
};

struct const_functor_by_const_value{
  void operator()(const ts) const { }
};

struct const_functor_by_reference{
  void operator()(ts&) const { }
};

struct const_functor_by_const_reference{
  void operator()(const ts&) const { }
};



struct overload_functor_by_value{
  void operator()(ts) { }
  void operator()(ts) const { }
};

struct overload_functor_by_const_value{
  void operator()(const ts) { }
  void operator()(const ts) const { }
};

struct overload_functor_by_reference{
  void operator()(ts&) { }
  void operator()(ts&) const { }
};

struct overload_functor_by_const_reference{
  void operator()(const ts&) { }
  void operator()(const ts&) const { }
};


/*
 * Returns true if T is a function which matches void(const ts&)
 * or if T is a functor with a void operator()(const ts&) const
 */
template <typename T>
int test_function_is_const_ref(T t) {
  return graphlab::test_function_or_const_functor_1<T,
                                                    void(const ts&), /* function form*/
                                                    void,            /* return type */
                                                    const ts&        /* argument 1 */
                                                    >::value;
}

int main(int argc, char** argv) {
  std::cout << test_function_is_const_ref(by_value) << std::endl;
  std::cout << test_function_is_const_ref(by_const_value) << std::endl;
  std::cout << test_function_is_const_ref(by_reference) << std::endl;
  std::cout << test_function_is_const_ref(by_const_reference) << std::endl;

  std::cout << test_function_is_const_ref(functor_by_value()) << std::endl;
  std::cout << test_function_is_const_ref(functor_by_const_value()) << std::endl;
  std::cout << test_function_is_const_ref(functor_by_reference()) << std::endl;
  std::cout << test_function_is_const_ref(functor_by_const_reference()) << std::endl;

  std::cout << test_function_is_const_ref(const_functor_by_value()) << std::endl;
  std::cout << test_function_is_const_ref(const_functor_by_const_value()) << std::endl;
  std::cout << test_function_is_const_ref(const_functor_by_reference()) << std::endl;
  std::cout << test_function_is_const_ref(const_functor_by_const_reference()) << std::endl;

  std::cout << test_function_is_const_ref(overload_functor_by_value()) << std::endl;
  std::cout << test_function_is_const_ref(overload_functor_by_const_value()) << std::endl;
  std::cout << test_function_is_const_ref(overload_functor_by_reference()) << std::endl;
  std::cout << test_function_is_const_ref(overload_functor_by_const_reference()) << std::endl;
}
