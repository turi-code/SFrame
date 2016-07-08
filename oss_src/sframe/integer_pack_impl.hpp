#ifndef GRAPHLAB_SFRAME_INTEGER_PACK_IMPL_HPP
#define GRAPHLAB_SFRAME_INTEGER_PACK_IMPL_HPP

/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <serialization/serialization_includes.hpp>
namespace graphlab {
namespace integer_pack {

static inline size_t pack_1(const uint64_t* src, size_t srclen, uint8_t* out) {
  uint8_t* initial_out = out;
  size_t n = (srclen + 7) / 8;
  uint8_t c = 0;
  switch(srclen % 8) {
    case 0: do { c |= ((*src++));
    case 7:      c |= ((*src++)) << 1;
    case 6:      c |= ((*src++)) << 2;
    case 5:      c |= ((*src++)) << 3;
    case 4:      c |= ((*src++)) << 4;
    case 3:      c |= ((*src++)) << 5;
    case 2:      c |= ((*src++)) << 6;
    case 1:      c |= ((*src++)) << 7;
                 (*out++) = c;
                 c = 0;
   } while(--n > 0);
  }
  return out - initial_out;
}

static inline size_t pack_2(const uint64_t* src, size_t srclen, uint8_t* out) {
  uint8_t* initial_out = out;
  size_t n = (srclen + 7) / 8;
  uint8_t c = 0;
  switch(srclen % 8) {
    case 0: do { c |= ((*src++));
    case 7:      c |= ((*src++)) << 2;
    case 6:      c |= ((*src++)) << 4;
    case 5:      c |= ((*src++)) << 6;
                 (*out++) = c;
                 c = 0;
    case 4:      c |= ((*src++));
    case 3:      c |= ((*src++)) << 2;
    case 2:      c |= ((*src++)) << 4;
    case 1:      c |= ((*src++)) << 6;
                 (*out++) = c;
                 c = 0;
   } while(--n > 0);
  }
  return out - initial_out;
}


static inline size_t pack_4(const uint64_t* src, size_t srclen, uint8_t* out) {
  uint8_t* initial_out = out;
  size_t n = (srclen + 7) / 8;
  uint8_t c = 0;
  switch(srclen % 8) {
    case 0: do { c |= ((*src++));
    case 7:      c |= ((*src++)) << 4;
                 (*out++) = c;
                 c = 0;
    case 6:      c |= ((*src++));
    case 5:      c |= ((*src++)) << 4;
                 (*out++) = c;
                 c = 0;
    case 4:      c |= ((*src++));
    case 3:      c |= ((*src++)) << 4;
                 (*out++) = c;
                 c = 0;
    case 2:      c |= ((*src++));
    case 1:      c |= ((*src++)) << 4;
                 (*out++) = c;
                 c = 0;
   } while(--n > 0);
  }
  return out - initial_out;
}

static inline size_t pack_8(const uint64_t* src, size_t srclen, uint8_t* out) {
  uint8_t* initial_out = out;
  const uint64_t* src_end = src + srclen;
  while(src != src_end) {
    (*out++) = (*src++);
  }
  return out - initial_out;
}


static inline size_t pack_16(const uint64_t* src, size_t srclen, uint16_t* out) {
  uint16_t* initial_out = out;
  const uint64_t* src_end = src + srclen;
  while(src != src_end) {
    (*out++) = (*src++);
  }
  return 2*(out - initial_out);
}

static inline size_t pack_32(const uint64_t* src, size_t srclen, uint32_t* out) {
  uint32_t* initial_out = out;
  const uint64_t* src_end = src + srclen;
  while(src != src_end) {
    (*out++) = (*src++);
  }
  return 4*(out - initial_out);
}


static inline void unpack_1(const uint8_t* src, size_t nout_values, uint64_t* out) {
  size_t n = (nout_values + 7) / 8;
  uint8_t c = (*src++);
  // the first byte, if incomplete, annoying is going to live
  // in the most significant bits of c. Thus if nout_value % 8 != 0,
  // I need to do a bit of shifting. 
  c >>= ((8 - (nout_values % 8)) % 8);
  switch(nout_values % 8) {
    do {         c = (*src++);
    case 0:      (*out++) = c & 1; c >>= 1;
    case 7:      (*out++) = c & 1; c >>= 1;
    case 6:      (*out++) = c & 1; c >>= 1;
    case 5:      (*out++) = c & 1; c >>= 1;
    case 4:      (*out++) = c & 1; c >>= 1;
    case 3:      (*out++) = c & 1; c >>= 1;
    case 2:      (*out++) = c & 1; c >>= 1;
    case 1:      (*out++) = c & 1; c >>= 1;
   } while(--n > 0);
  }
}


static inline void unpack_2(const uint8_t* src, size_t nout_values, uint64_t* out) {
  size_t n = (nout_values + 7) / 8;
  uint8_t c = (*src++);
  c >>= ((8 - 2 * (nout_values % 4)) % 8);
  switch(nout_values % 8) {
    do {         c = (*src++);
    case 0:      (*out++) = c & 3; c >>= 2;
    case 7:      (*out++) = c & 3; c >>= 2;
    case 6:      (*out++) = c & 3; c >>= 2;
    case 5:      (*out++) = c & 3; c >>= 2;
                 c = (*src++);
    case 4:      (*out++) = c & 3; c >>= 2;
    case 3:      (*out++) = c & 3; c >>= 2;
    case 2:      (*out++) = c & 3; c >>= 2;
    case 1:      (*out++) = c & 3; c >>= 2;
   } while(--n > 0);
  }
}


static inline void unpack_4(const uint8_t* src, size_t nout_values, uint64_t* out) {
  size_t n = (nout_values + 7) / 8;
  uint8_t c = (*src++);
  c >>= ((8 - 4 * (nout_values % 2)) % 8);
  switch(nout_values % 8) {
    do {         c = (*src++);
    case 0:      (*out++) = c & 15; c >>= 4;
    case 7:      (*out++) = c & 15; c >>= 4;
                 c = (*src++);
    case 6:      (*out++) = c & 15; c >>= 4;
    case 5:      (*out++) = c & 15; c >>= 4;
                 c = (*src++);
    case 4:      (*out++) = c & 15; c >>= 4;
    case 3:      (*out++) = c & 15; c >>= 4;
                 c = (*src++);
    case 2:      (*out++) = c & 15; c >>= 4;
    case 1:      (*out++) = c & 15; c >>= 4;
   } while(--n > 0);
  }
}


static inline void unpack_8(const uint8_t* src, size_t nout_values, uint64_t* out) {
  const uint8_t* src_end = src + nout_values;
  while(src != src_end) {
    (*out++) = (*src++);
  }
}


static inline void unpack_16(const uint16_t* src, size_t nout_values, uint64_t* out) {
  const uint16_t* src_end = src + nout_values;
  while(src != src_end) {
    (*out++) = (*src++);
  }
}

static inline void unpack_32(const uint32_t* src, size_t nout_values, uint64_t* out) {
  const uint32_t* src_end = src + nout_values;
  while(src != src_end) {
    (*out++) = (*src++);
  }
}


} // namespace integer_pack
} // namespace graphlab

#endif
