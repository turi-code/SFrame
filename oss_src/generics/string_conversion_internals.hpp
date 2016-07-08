/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <generics/gl_string.hpp> 

#ifndef GRAPHLAB_STRING_CONVERSION_INTERNALS_H_
#define GRAPHLAB_STRING_CONVERSION_INTERNALS_H_

namespace graphlab { namespace gl_string_internal { 

// as_string
template <typename V>
GL_HOT_INLINE_FLATTEN static inline
gl_string as_string(const char* fmt, V a) {
  gl_string s;
  s.resize(s.capacity());
  size_t available = s.size();
  while (true) {
    int status = snprintf(&s[0], available, fmt, a);
    if ( status >= 0 ) {
      size_t used = static_cast<size_t>(status);
      if ( used <= available ) {
        s.resize( used );
        break;
      }
      available = used; // Assume this is advice of how much space we need.
    } else {
      available = available * 2 + 1;
    }
    
    s.resize(available);
  }
  return s;
}

}}

#endif /* _STRING_CONVERSION_INTERNALS_H_ */
