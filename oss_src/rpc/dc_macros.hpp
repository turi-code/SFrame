/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_RPC_DC_MACROS_HPP
#define GRAPHLAB_RPC_DC_MACROS_HPP

/**
 * Convenience macro to use when issuing RPC calls.
 *
 * All of RPC call functions (for instance \ref graphlab::distributed_control::remote_call,
 * or \ref graphlab::dc_dist_object::remote_call) are templatized over both the
 * type of the function being called, as well as the function pointer itself.
 *
 * However, since the function pointer cannot be deduced, it must be provided
 * explicitly, thus resulting in a rather annoying syntax for calling the
 * remote_call. i.e.
 * \code
 * // A print function is defined
 * void print(std::string s) {
 *   std::cout << s << "\n";
 * }
 *
 * dc.template remote_call<decltype(print), print>(1, "hello");
 * \endcode
 *
 * This macro provides a convenience wrapper around the syntax to allow:
 * \code
 * dc.RPC_CALL(remote_call, print)(1, "hello");
 * \endcode
 *
 *
 *
 */
#define RPC_CALL(calltype, ...) template calltype<decltype(&__VA_ARGS__), &__VA_ARGS__>

#endif
