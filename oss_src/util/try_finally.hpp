/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#ifndef GRAPHLAB_TRY_FINALLY_H_
#define GRAPHLAB_TRY_FINALLY_H_

#include <vector>
#include <functional>

namespace graphlab {

/**  Class to use the garuanteed destructor call of a scoped variable
 *   to simulate a try...finally block.  This is the standard C++ way
 *   of doing that.
 *
 *   Use:
 *
 *   When you want to ensure something is executed, even when
 *   exceptions are present, create a scoped_finally structure and add
 *   the appropriate cleanup functions to it.  When this structure
 *   goes out of scope, either due to an exception or the code leaving
 *   that scope, these functions are executed.
 *
 *   This is useful for cleaning up locks, etc.
 */
class scoped_finally {
 public:

  scoped_finally()
  {}

  scoped_finally(std::vector<std::function<void()> > _f_v)
      : f_v(_f_v)
  {}

  scoped_finally(std::function<void()> _f)
      : f_v{_f}
  {}

  void add(std::function<void()> _f) {
    f_v.push_back(_f);
  }

  void execute_and_clear() {
    for(size_t i = f_v.size(); (i--) != 0;) {
      f_v[i]();
    }
    f_v.clear();
  }

  ~scoped_finally() {
    execute_and_clear();
  }

 private:
  std::vector<std::function<void()> > f_v;
};

}







#endif /* _TRY_FINALLY_H_ */
