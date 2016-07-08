/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#ifndef CPPIPC_UTIL_GENERICS_MEMBER_FUNCTION_RETURN_TYPE_HPP
#define CPPIPC_UTIL_GENERICS_MEMBER_FUNCTION_RETURN_TYPE_HPP

namespace cppipc {
namespace detail {


template <typename MemFn>
struct member_function_return_type {
  typedef typename boost::remove_member_pointer<MemFn>::type fntype;
  typedef typename boost::function_traits<fntype>::result_type type;
};

} // detail
} // cppipc

#endif
