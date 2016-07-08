/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
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


#ifndef GRAPHLAB_TEST_FUNCTION_OR_FUNCTOR_TYPE_HPP
#define GRAPHLAB_TEST_FUNCTION_OR_FUNCTOR_TYPE_HPP
#include <boost/type_traits/is_same.hpp>
#include <boost/type_traits/remove_pointer.hpp>

namespace graphlab {

  /**
   * \brief This tests that a type F matches a specification
   * for a function type or implements a specification for a functor type.
   *
   * Where F is the type to test:
   * \code
   * test_function_or_functor_1<F, PreferredFunctionForm, RetType, Arg1>::value
   * \endcode
   * 
   * is true if any of the following is true:
   * \li F is the same type as PreferredFunctionForm
   * \li F is the same type as PreferredFunctionForm*
   * \li F implements an F::operator() with the prototype RetType F::operator()(Arg1)
   *
   * For instance,
   * \code
   * test_function_or_functor_1<F, void(const int&), void, const int&>::value
   * \endcode
   *
   * is true if any of the following is true:
   * \li F is a void(const int&)
   * \li F is a void(*)(const int&)
   * \li F is a class which implements void F::operator()(const int&)
   *
   * There is an additional "const" variant test_function_or_const_functor_1
   * which requires operator() to be a const function. i.e.:
   * \code
   * test_function_or_const_functor_1<F, void(const int&), void, const int&>::value
   * \endcode
   *
   * is true if any of the following is true:
   * \li F is the same type as PreferredFunctionForm
   * \li F is the same type as PreferredFunctionForm*
   * \li F implements an F::operator() with the prototype RetType F::operator()(Arg1) const
   *
   * Both variants have a 2 argument version test_function_or_functor_2 and
   * test_function_or_const_functor_2 which take an additional template
   * argument for the second argument.
   */
  template <typename F,
            typename PreferredFunctionForm,
            typename RetType,
            typename Arg1>
  struct test_function_or_functor_1 {

    // test if the functor type matches
    template <typename T, RetType (T::*)(Arg1)>
    struct SFINAE1 {};

    template <typename T>
    static char test1(SFINAE1<T, &T::operator()>*);

    template <typename T>
    static int test1(...);

    static const bool value = ((sizeof(test1<F>(0)) == sizeof(char)) ||
                               boost::is_same<F, PreferredFunctionForm>::value ||
                                boost::is_same<typename boost::remove_pointer<F>::type, PreferredFunctionForm>::value);
  };


  /**
   * \copydoc test_function_or_functor_1
   */
  template <typename F,
            typename PreferredFunctionForm,
            typename RetType,
            typename Arg1>
  struct test_function_or_const_functor_1 {

    // test if the functor type matches
    template <typename T, RetType (T::*)(Arg1) const>
    struct SFINAE1 {};

    template <typename T>
    static char test1(SFINAE1<T, &T::operator()>*);

    template <typename T>
    static int test1(...);
    
    static const bool value = ((sizeof(test1<F>(0)) == sizeof(char)) ||
                               boost::is_same<F, PreferredFunctionForm>::value ||
                                boost::is_same<typename boost::remove_pointer<F>::type, PreferredFunctionForm>::value);
  };



  /**
   * \copydoc test_function_or_functor_1
   */
  template <typename F,
            typename PreferredFunctionForm,
            typename RetType,
            typename Arg1,
            typename Arg2>
  struct test_function_or_functor_2 {

    // test if the functor type matches
    template <typename T, RetType (T::*)(Arg1, Arg2)>
    struct SFINAE1 {};

    template <typename T>
    static char test1(SFINAE1<T, &T::operator()>*);

    template <typename T>
    static int test1(...);

    static const bool value = ((sizeof(test1<F>(0)) == sizeof(char)) ||
                               boost::is_same<F, PreferredFunctionForm>::value ||
                                boost::is_same<typename boost::remove_pointer<F>::type, PreferredFunctionForm>::value);
  };

  
  /**
   * \copydoc test_function_or_functor_1
   */
  template <typename F,
            typename PreferredFunctionForm,
            typename RetType,
            typename Arg1,
            typename Arg2>
  struct test_function_or_const_functor_2 {

    // test if the functor type matches
    template <typename T, RetType (T::*)(Arg1, Arg2) const>
    struct SFINAE1 {};

    template <typename T>
    static char test1(SFINAE1<T, &T::operator()>*);

    template <typename T>
    static int test1(...);

    static const bool value = ((sizeof(test1<F>(0)) == sizeof(char)) ||
                               boost::is_same<F, PreferredFunctionForm>::value ||
                                boost::is_same<typename boost::remove_pointer<F>::type, PreferredFunctionForm>::value);
  };

  
  
} // namespace graphlab
#endif
