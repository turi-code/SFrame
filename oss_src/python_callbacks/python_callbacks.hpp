#ifndef GRAPHLAB_PYTHON_CALLBACKS_H_
#define GRAPHLAB_PYTHON_CALLBACKS_H_

#include <util/code_optimization.hpp>
#include <string>
#include <export.hpp>

/** Provides a generic interface to call cython functions (which can
 *  in turn call python functions) from the C++ code and properly
 *  handle exceptions.
 */

namespace graphlab { namespace python { 

struct python_exception_info {
  std::string exception_pickle;
  std::string exception_string;
};

// Registers the exception 
void register_python_exception(const python_exception_info*);
extern bool _python_exception_occured; 

void _process_registered_exception() GL_COLD_NOINLINE;

/**  Processes exceptions raised by the above functions.  To use, do
 *
 * On the cython side:
 * 
 * from cy_callbacks cipmort register_exception
 * 
 * cdef void my_func(...):
 *     try:
 *         # Do stuff...
 * 
 *     except Exception, e:
 *         register_exception(e)
 *         return
 *
 *
 *   On the C++ side. 
 *
 *   cython_function_struct.my_func(...);
 *   check_for_python_exception();
 */
static inline GL_HOT_INLINE void check_for_python_exception() {
  if(UNLIKELY(_python_exception_occured)) {
    _process_registered_exception();
  }
}

}}

#endif /* GRAPHLAB_PYTHON_CALLBACKS_H_ */

