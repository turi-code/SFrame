/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
/**  
 * Copyright (c) 2009 Carnegie Mellon University. 
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS
 *  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied.  See the License for the specific language
 *  governing permissions and limitations under the License.
 *
 * For more about this software visit:
 *
 *      http://www.graphlab.ml.cmu.edu
 *
 */


#include <cstdlib>
#include <cstring>
#include <logger/assertions.hpp>
#include <rpc/circular_char_buffer.hpp>

namespace graphlab {
  
  circular_char_buffer::circular_char_buffer(std::streamsize initial) {
    initial = std::max<size_t>((size_t)initial, 4);
    // allocate something to start with
    buffer = (char*)malloc(initial);
    head = 0;
    tail = 0;
    bufsize = initial;
    len = 0;
  }

  circular_char_buffer::circular_char_buffer(const circular_char_buffer &src) {
    // allocate minimum of 4 bytes
    bufsize = std::max<size_t>(src.size(), 4);
    buffer = (char*)malloc(bufsize);
  
    // copy the buffer in src
    src.peek(buffer, src.size());
    // set the lengths
    len = src.size();
    tail = src.size();
    head = 0;
    if (tail == bufsize) tail = 0;
  }
  
  circular_char_buffer& 
  circular_char_buffer::operator=(const circular_char_buffer& src) {
    // reset head and tail
    clear();
    // make sure we have enough room
    reserve(src.size());
    src.peek(buffer, src.size());
    len = src.size();
    tail = src.size();
    if (tail == bufsize) tail = 0;
    return *this;
  }
  
  void circular_char_buffer::reserve(std::streamsize s) {
    // minimum of 4 bytes. disallow reserve of 0
    if (s <= 4) s = 4;
    // do nothing if s is smaller than the current buffer size
    if (s <= bufsize) return;
  
    // do a reallocation
    buffer = (char*)realloc((void*)buffer, s);

    // now, we need to be careful to reposition the head and tail
    // there are 2 cases
  
    // case 1:  no loop around,
    //         Easiest case. do nothing. just update bufsize
    // case 2:  we have a loop around,
    //         copy the left side of the loop around to the end. 
    if (tail >= head) {
      bufsize = s;
    }
    else {
      // how much excess space do we have now?
      size_t excess = (size_t)s - bufsize;
      // move up to excess bytes to the end 
      size_t movebytes = std::min<size_t>(tail, excess);
      memcpy(buffer + bufsize, buffer, movebytes); 
      // move the remaining bytes to the start of the buffer
      memmove(buffer, buffer+movebytes, tail - movebytes);
      // update buftail
      // if movebytes == tail, then tail has been wiped out
      // and it is no longer a looparound
      bufsize = s;    
      tail = (head + len) % bufsize;

    }
    consistency_check();
  }

  void circular_char_buffer::squeeze() {
    // squeeze to a minimum of 4 bytes
    if (bufsize <= 4) return;
    // 2 cases
    // case 1: no loop around
    // case 2: loop around. Easiest solution is to allocate a new buffer
    if (tail >= head) {
      if (head > 0) memmove(buffer, buffer+head, len);
      std::streamsize efflen = std::max(len + 1, std::streamsize(4));
      buffer = (char*)realloc((void*)buffer, efflen);
      head = 0;
      tail = len;
      bufsize = efflen;
    }
    else {
      // allocate a new buffer
      std::streamsize efflen = std::max(len + 1, std::streamsize(4));
      char *newbuf = (char*)malloc(efflen);
      // read into this buffer
      peek(newbuf, len);
      // free the old buffer
      free(buffer);
      buffer = newbuf;
      head = 0;
      tail = len;
      bufsize = efflen;
    }
    consistency_check();
  }


  void circular_char_buffer::align() {
    // squeeze to a minimum of 4 bytes
    if (bufsize <= 4) return;
    // 2 cases
    // case 1: no loop around
    // case 2: loop around. Easiest solution is to allocate a new buffer
    if (tail >= head) {
      if (head > 0) memmove(buffer, buffer+head, len);
      head = 0;
      tail = len;
    }
    else {
      // allocate a new buffer
      char *newbuf = (char*)malloc(bufsize);
      // read into this buffer
      peek(newbuf, len);
      // free the old buffer
      free(buffer);
      buffer = newbuf;
      head = 0;
      tail = len;
    }
  }

  bool circular_char_buffer::align_requires_alloc() {
    // squeeze to a minimum of 4 bytes
    if (bufsize <= 4) return false;
    // 2 cases
    // case 1: no loop around
    // case 2: loop around. Easiest solution is to allocate a new buffer
    if (tail >= head) {
      return false;
    }
    else {
      return true;
    }
  }
  std::streamsize circular_char_buffer::peek(char* c, 
                                             std::streamsize clen) const {
    std::streamsize readlen = std::min(clen, len);
    // eliminate the case where head == tail, but buffer is empty
    if (readlen == 0) return 0;
  
    // first copy from head to end of buffer
    std::streamsize firstcopy = std::min(bufsize - head, readlen);
    memcpy(c, buffer+head, firstcopy);
    if (firstcopy == readlen) return readlen;
    // second copy from beginning of buffer to tail
    std::streamsize secondcopy = std::min(tail, readlen - firstcopy);
    memcpy(c+firstcopy, buffer, secondcopy);
    consistency_check();

    return readlen;

  }

  std::streamsize circular_char_buffer::skip(std::streamsize clen) {
    std::streamsize readlen = std::min(clen, len);
    head += readlen;
    if (head >= bufsize) head -= bufsize;
    len -= readlen;
    consistency_check(); 
    return readlen;
  }

  std::streamsize circular_char_buffer::read(char* c, 
                                             std::streamsize clen) {
    if (len == 0) return -1;
    std::streamsize readlen = peek(c, clen);
    skip(readlen);
    consistency_check(); 

    return readlen;
  }

  std::streamsize circular_char_buffer::peek(std::string &c, 
                                             std::streamsize clen) const{
    c.clear();
    c.resize(clen);
    return peek(const_cast<char*>(c.c_str()), clen);
  }

  std::streamsize circular_char_buffer::read(std::string &c, 
                                             std::streamsize clen) {
    c.clear();
    c.resize(clen);
    return read(const_cast<char*>(c.c_str()), clen);
  }


  std::streamsize circular_char_buffer::write(const char* c, 
                                              std::streamsize clen) {
    // if we do not have enough capacity.
    // make sure we have enough capacity
    reserve(clen + len + 1);
    len += clen;
  
    std::streamsize firstcopy = std::min(clen, bufsize - tail);
    memcpy(buffer + tail, c, firstcopy);
    tail += firstcopy;
    if (tail == bufsize) tail = 0;
    if (firstcopy == clen) {
      consistency_check(); 
      return clen;
    }
  
    std::streamsize secondcopy = clen - firstcopy;
    memcpy(buffer, c + firstcopy, secondcopy);
    tail += secondcopy;
    consistency_check(); 
    return clen;
  }

  void circular_char_buffer::clear() {
    head = 0; tail = 0; len = 0;
  }

  circular_char_buffer::~circular_char_buffer() {
    free(buffer);
  }

  std::streamsize circular_char_buffer::introspective_read(char* &s) {
    if (len == 0) {
      s = NULL;
      return 0;
    }
    s = buffer + head;
    // how much we do read?
    // we can go up to the end of the buffer, or until a looparound
    // case 1: no looparound
    // case 2: looparound
    std::streamsize readlen = 0;
    if (tail > head) {
      readlen = tail - head;
    }
    else {
      readlen = bufsize - head;
    }
    skip(readlen);
    return readlen;
  }

  std::streamsize circular_char_buffer::introspective_read(char* &s, std::streamsize clen) {
    if (len == 0) {
      s = NULL;
      return 0;
    }
    s = buffer + head;
    // how much we do read?
    // we can go up to the end of the buffer, or until a looparound
    // case 1: no looparound
    // case 2: looparound
    std::streamsize readlen = 0;
    if (tail > head) {
      readlen = tail - head;
    }
    else {
      readlen = bufsize - head;
    }
    if (clen < readlen) readlen = clen;

    skip(readlen);
    return readlen;
  }


  std::streamsize circular_char_buffer::introspective_write(char* &s) {
    s = buffer + tail;
    if (tail >= head) {
      // case 1. no looparound. 
      return bufsize - tail - (head==0);
    }
    else {
      // case 2 looparound
      return head - tail - 1;
    }
  }
  
  void circular_char_buffer::advance_write(std::streamsize bytes) {
    tail += bytes;
    if (tail >= bufsize) tail -= bufsize;
    len += bytes;
    consistency_check();
  }
  
};

