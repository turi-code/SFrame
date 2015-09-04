/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#ifndef CPPIPC_UTIL_GENERICS_TUPLE_HPP
#define CPPIPC_UTIL_GENERICS_TUPLE_HPP
#include <tuple>
namespace cppipc {

namespace tuple_detail {
template <typename R, typename... T>
std::tuple<T...> __function_args_to_tuple(R (*)(T...)) {
  return std::tuple<T...>();
}
} // namespace detail


/**
 * \ingroup cppipc
 * Converts the arguments of a function type to a tuple over the argument type.
 */
template <typename Fn>
struct function_args_to_tuple {
  typedef decltype(tuple_detail::__function_args_to_tuple(reinterpret_cast<Fn*>(NULL))) type;
};


template<typename T>
struct left_shift_tuple { 
  typedef std::tuple<> type;
};



/**
 * \ingroup cppipc
 * Returns a tuple without the left most element of the tuple.
 * left shifting of an empty tuple returns an empty tuple.
 */
template<typename T, typename... Ts>
struct left_shift_tuple <std::tuple<T, Ts...> > {
    typedef std::tuple<Ts...> type;
};

} // namespace cppipc
#endif
