/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_SFRAME_OUTPUT_ITERATOR_HPP
#define GRAPHLAB_SFRAME_OUTPUT_ITERATOR_HPP
#include <iterator>

namespace graphlab {
class sframe_rows;

template <typename T, typename ConstRefFunction, typename MoveFunction, typename SFrameRowsFunction>
class sframe_function_output_iterator {
  typedef sframe_function_output_iterator self;
public:
  typedef std::output_iterator_tag iterator_category;
  typedef void                value_type;
  typedef void                difference_type;
  typedef void                pointer;
  typedef void                reference;

  explicit sframe_function_output_iterator() {}

  explicit sframe_function_output_iterator(const ConstRefFunction& f, 
                                           const MoveFunction& f2,
                                           const SFrameRowsFunction& f3)
    : m_f(f), m_f2(f2), m_f3(f3) {}

  struct output_proxy {
    output_proxy(const ConstRefFunction& f, 
                 const MoveFunction& f2,
                 const SFrameRowsFunction& f3) : m_f(f), m_f2(f2), m_f3(f3) { }

    output_proxy& operator=(const T& value) {
      m_f(value);
      return *this; 
    }

    output_proxy& operator=(T&& value) {
      m_f2(std::move(value));
      return *this; 
    }

    output_proxy& operator=(const sframe_rows& value) {
      m_f3(value);
      return *this; 
    }

    const ConstRefFunction& m_f;
    const MoveFunction& m_f2;
    const SFrameRowsFunction& m_f3;
  };
  output_proxy operator*() { return output_proxy(m_f, m_f2, m_f3); }
  self& operator++() { return *this; } 
  self& operator++(int) { return *this; }
private:
  ConstRefFunction m_f;
  MoveFunction m_f2;
  SFrameRowsFunction m_f3;
};

} // namespace graphlab

#endif // GRAPHLAB_SFRAME_OUTPUT_ITERATOR_HPP

