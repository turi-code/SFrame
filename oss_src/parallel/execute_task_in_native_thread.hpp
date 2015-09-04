/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_PARALLEL_EXECUTE_TASK_IN_NATIVE_THREAD_HPP
#define GRAPHLAB_PARALLEL_EXECUTE_TASK_IN_NATIVE_THREAD_HPP
#include <functional>

namespace graphlab {

/**
 * Takes a function and executes it in a native stack space.
 * Used to get by some libjvm oddities when using coroutines / fibers.
 *
 * Returns an exception if an exception was thrown while executing the inner task.
 */
std::exception_ptr execute_task_in_native_thread(const std::function<void(void)>& fn);

namespace native_exec_task_impl {
template <typename T> 
struct value_type {
  T ret;

  T get_result() {
    return ret;
  }

  template <typename F, typename ... Args>
  void run_as_native(F f, Args... args) {
    auto except = execute_task_in_native_thread([&](void)->void {
      ret = f(args...);
    });
    if (except) std::rethrow_exception(except);
  }
};

template <> 
struct value_type<void> {

  void get_result() { }

  template <typename F, typename ... Args>
  void run_as_native(F f, Args... args) {
    auto except = execute_task_in_native_thread([&](void)->void {
      f(args...);
    });
    if (except) std::rethrow_exception(except);
  }
};
} // namespace native_exec_task_impl
/**
 * Takes a function call and runs it in a native stack space.
 * Used to get by some libjvm oddities when using coroutines / fibers.
 *
 * Returns an exception if an exception was thrown while executing the inner task.
 */
template <typename F, typename ... Args>
typename std::result_of<F(Args...)>::type run_as_native(F f, Args... args) {
  native_exec_task_impl::value_type<typename std::result_of<F(Args...)>::type> result;

  result.template run_as_native<F, Args...>(f, args...);

  return result.get_result();
}

} // graphlab
#endif
