#ifndef GRAPHLAB_TYPE_TRAITS_H_
#define GRAPHLAB_TYPE_TRAITS_H_

#include <type_traits>
#include <map>
#include <unordered_map>
#include <vector>
#include <tuple>

// namespace std {

// template <typename T, typename Alloc> class vector;
// template <typename... T> class map;
// template <typename T, typename U, class typename Alloc> class unordered_map;

// }

namespace graphlab { 

template <typename T> class gl_vector;

struct invalid_type {};
struct _invalid_type_base { typedef invalid_type type; }; 

////////////////////////////////////////////////////////////////////////////////
// Is it an std::vector?

template<class... T> struct is_std_vector : public std::false_type {};
template<typename... A> struct is_std_vector<std::vector<A...> > : public std::true_type {};

////////////////////////////////////////////////////////////////////////////////
// Is it a gl_vector?

template<class... T> struct is_gl_vector : public std::false_type {};
template<typename... A> struct is_gl_vector<gl_vector<A...> > : public std::true_type {};

////////////////////////////////////////////////////////////////////////////////
// Is it any sort of vector?

template<typename... A> struct is_vector {
  static constexpr bool value = (is_std_vector<A...>::value || is_gl_vector<A...>::value); 
}; 

////////////////////////////////////////////////////////////////////////////////
// Is it any sort of deque?

template<typename... T> struct is_deque : public std::false_type {};
template<typename... A> struct is_deque<std::deque<A...> > : public std::true_type {};

////////////////////////////////////////////////////////////////////////////////
// Is it any sort of list?

template<typename... T> struct is_list : public std::false_type {};
template<typename... A> struct is_list<std::list<A...> > : public std::true_type {};

////////////////////////////////////////////////////////////////////////////////
// Is it any of the above sequence operators?

template<typename... A> struct is_sequence_container {
  static constexpr bool value =
      (is_vector<A...>::value || is_deque<A...>::value || is_list<A...>::value);
};

////////////////////////////////////////////////////////////////////////////////
// Is it an std::map?

template<class T> struct is_std_map : public std::false_type {};
template<typename... A> struct is_std_map<std::map<A...> > : public std::true_type {};

////////////////////////////////////////////////////////////////////////////////
// Is it an std::unordered_map?

template<class T> struct is_std_unordered_map : public std::false_type {};
template<typename... A> struct is_std_unordered_map<std::unordered_map<A...> > : public std::true_type {};

////////////////////////////////////////////////////////////////////////////////
// Is it an boost::unordered_map?

template<class T> struct is_boost_unordered_map : public std::false_type {};
template<typename... A> struct is_boost_unordered_map<boost::unordered_map<A...> > : public std::true_type {};

////////////////////////////////////////////////////////////////////////////////
// Is it any type of map?

template<typename... A> struct is_map {
  static constexpr bool value = (is_std_map<A...>::value
                                 || is_std_unordered_map<A...>::value
                                 || is_boost_unordered_map<A...>::value);
};

////////////////////////////////////////////////////////////////////////////////
// Is it an std::pair?

template<class T> struct is_std_pair : public std::false_type {};
template<typename... A> struct is_std_pair<std::pair<A...> > : public std::true_type {};


////////////////////////////////////////////////////////////////////////////////
// Is it an std::string?

template<class... T> struct is_std_string : public std::false_type {};
template <> struct is_std_string<std::string> : public std::true_type {};

////////////////////////////////////////////////////////////////////////////////
// Is it a gl_string?

template<class... T> struct is_gl_string : public std::false_type {};
template <> struct is_gl_string<gl_string> : public std::true_type {};

////////////////////////////////////////////////////////////////////////////////
// Is it any sort of string?

template<typename... A> struct is_string {
  static constexpr bool value = (is_std_string<A...>::value || is_gl_string<A...>::value); 
}; 


////////////////////////////////////////////////////////////////////////////////
// Extract the first nested type from a template parameterized type
// definition; return invalid_type on failure.

template<class T> struct first_nested_type {
  typedef invalid_type type;
};

template<>
template<class T, typename... A, template <typename...> class C>
struct first_nested_type<C<T, A...> > {
  typedef T type;
};

template<>
template<class T, template <typename T> class C>
struct first_nested_type<C<T> > {
  typedef T type;
};

////////////////////////////////////////////////////////////////////////////////
// Extract the second nested type from a template parameterized type
// definition; return invalid_type on failure.

template<class T> struct second_nested_type {
  typedef invalid_type type;
};

template<>
template<class T, class U, typename... A, template <typename...> class C>
struct second_nested_type<C<T, U, A...> > {
  typedef U type;
};

template<>
template<class T, class U, template <typename...> class C>
struct second_nested_type<C<T, U> > {
  typedef U type;
};


////////////////////////////////////////////////////////////////////////////////
// All true

template <template <typename> class P, typename... Args>
struct all_true : public std::false_type {};

template <template <typename> class P>
template <class T>
struct all_true<P, T> : public P<T> {}; 


template<template <typename> class P>
template<class T, typename... Args>
struct all_true<P, T, Args...> {
  static constexpr bool value = (P<T>::value && all_true<P, Args...>::value);
};



////////////////////////////////////////////////////////////////////////////////
// 

template <bool, template <typename...> class Cond, typename... Args> struct conditional_test
    : public std::false_type {}; 

template <template <typename...> class Cond, typename... Args>
struct conditional_test<true, Cond, Args...> {

  template <template <typename...> class _Cond, typename... _Args>
  static constexpr bool _value(typename std::enable_if<_Cond<_Args...>::value, int>::type = 0) {
    return true;
  }

  template <template <typename...> class _Cond, typename... _Args>
  static constexpr bool _value(typename std::enable_if<!_Cond<_Args...>::value, int>::type = 0) {
    return false;
  }
  
  static constexpr bool value = _value<Cond, Args...>();
};

////////////////////////////////////////////////////////////////////////////////
// Is tuple

template<class T> struct is_tuple : public std::false_type {};
template<typename... A> struct is_tuple<std::tuple<A...> > : public std::true_type {};

////////////////////////////////////////////////////////////////////////////////
// or, and; with 
 

template<typename T> struct swallow_to_false : std::false_type {};


}

#endif /* GRAPHLAB_TYPE_TRAITS_H_ */
