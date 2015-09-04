/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_LAZY_EVAL_OPERATION_HPP
#define GRAPHLAB_LAZY_EVAL_OPERATION_HPP
#include <string>
#include <vector>
#include <iostream>

namespace graphlab {

/**
 * \ingroup unity
 * The base class for describing lazy operations using the lazy_operation_dag
 * system.
 *
 * \tparam T The object type tracked in the lazy operation DAG
 */
template <typename T>
struct lazy_eval_operation_base {
  typedef T value_type;
  /**
   * Number of arguments in the operation. For instance, a simple 
   * transformation (like "add_row") is a unary transform, and this 
   * function will return 1. A "join" is a binary transform, and will return 2.
   * Finally, parent-less operations like "load_from_file" will return 0.
   *
   * The only valid values at this time are 0, 1 or 2.
   */
  virtual size_t num_arguments() = 0;

  /**
   * Printable name of the operation
   */
  virtual std::string name() const { return std::string("");}

  /**
   * Execute the operation on the object, and the parents provided.
   * The size of the "parents" list is max(#arguments - 2, 0).
   *
   *  - For the Nullary function (function of 0 arguments, i.e. o = f() ) the
   *  size of the parents list is empty, and the operation should be
   *  performed on the output object directly.
   *  - For the Unary function (function of 1 argument. i.e. o = f(a1) ) the
   *  output object is the "parent" object , and the operation should be 
   *  performed inplace. i.e, it shoud really compute ( o = f(o) )
   *  - For the Binary function (function of 2 arguments) ( o = f(a1, a2) )
   *  the output object is the also the 1st parent, and the ancestor list
   *  contains a pointer to the 2nd parent. i.e. it should really compute
   *  ( o = f(o, a2) )
   *  - Operations of higher order generalize and extend accordingly.
   */
  virtual void execute(T& output,
                       const std::vector<T*>& parents) = 0;

  virtual ~lazy_eval_operation_base() {}
};

} // namespace graphlab
#endif
