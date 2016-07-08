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


#ifndef GRAPHLAB_CIRCULAR_CHAR_BUFFER_HPP
#define GRAPHLAB_CIRCULAR_CHAR_BUFFER_HPP
#include <string>
#include <iostream>  
#include <logger/assertions.hpp>
#include <boost/iostreams/stream.hpp>
#include <boost/iostreams/categories.hpp>
namespace graphlab {
  
  /**
   * \ingroup rpc
   * \internal
   A self-resizing circular buffer of characters
  */
  class circular_char_buffer {
  public:
   
    /// Creates a circular buffer with some initial capacity
    circular_char_buffer(std::streamsize initialsize = 1024);
    /// copy constructor
    circular_char_buffer(const circular_char_buffer &src);
    /// assignment operator
    circular_char_buffer& operator=(const circular_char_buffer& src);
  
    /// destructors
    ~circular_char_buffer();
  
    /// writes len bytes into the buffer
    std::streamsize write(const char* c, std::streamsize clen);
  
    /** tries to peek up to 'len' bytes from the buffer.
        the actual number of bytes read will be returned.
        This is a non-destructive operation */
    std::streamsize peek(char* c, std::streamsize clen) const;
  
    /** reads up to 'len' bytes from the buffer.
        the actual number of bytes read will be returned.
        This is a destructive operation */
    std::streamsize read(char* c, std::streamsize clen);

  
    /** tries to peek up to 'len' bytes from the buffer.
        the actual number of bytes read will be returned.
        This is a non-destructive operation. string overload of peek() */
    std::streamsize peek(std::string &s, std::streamsize clen) const;
  
    /** reads up to 'len' bytes from the buffer.  the actual number of
        bytes read will be returned.  This is a destructive
        operation. string overload of read() */
    std::streamsize read(std::string &s, std::streamsize clen);  
  
    /** skip some number of input bytes. Returns the number of bytes
        actually skipped*/
    std::streamsize skip(std::streamsize clen);
  
    /** reserves at least s bytes of capacity. Tries to perform as few
        memory copies as possible. No change is made if s is smaller
        than the current capacity. */
    void reserve(std::streamsize s);
  
    /** Squeezes out all empty capacity in the buffer so that
        the capacity is the same as the length of the buffer */
    void squeeze();

  
    /** Rotates the buffer so that the head is at index 0.
        buffer reserved size is preserved*/
    void align();


    /** Returns true if realignment requires a reallocation */
    bool align_requires_alloc();
  
    /**
     * Returns a pointer (through s) and a length of the read.  This
     * pointer is a direct pointer into the internal buffer of this
     * datastructure. The pointer is valid as long as no other
     * operations are performed on this structure.  The length of the
     * introspective_read may be less than the actual length of the
     * buffer. Multiple calls to introspective_read may be necessary
     * to read all data in the buffer. If the function returns 0, the
     * buffer is empty.
     */
    std::streamsize introspective_read(char* &s);
  
    /**
     * Returns a pointer (through s) and a length of the read.  This
     * pointer is a direct pointer into the internal buffer of this
     * datastructure. The pointer is valid as long as no other
     * operations are performed on this structure.  The length of the
     * introspective_read may be less than the number of bytes
     * requested. Multiple calls to introspective_read may be
     * necessary to read all data in the buffer. If the function
     * returns 0, the buffer is empty.
     */
    std::streamsize introspective_read(char* &s, std::streamsize clen);
  
    /**
       Returns a pointer to the next empty region of the buffer.  The
       return value is the maximum contigious length writable.  When
       writes complete, advance_write should be used to integrate the
       written bytes
    */
    std::streamsize introspective_write(char* &s);
  
    void advance_write(std::streamsize bytes);
  
    inline void consistency_check() const {
      /* ASSERT_GE(head, 0); ASSERT_GE(tail, 0);
         ASSERT_LT(head, bufsize); ASSERT_LE(tail, bufsize);
         if (tail > head) ASSERT_EQ(tail - head, len);
         else if (head < tail) ASSERT_EQ(head + bufsize - tail, len);
         else if (head == tail) ASSERT_EQ(len, 0); */
    }
  
    /** clears the stream */
    void clear();
  
    /** Gets the number of characters in the stream */
    inline std::streamsize size() const {
      return len;
    }
  
    /** Gets the size of the buffer. 
        \note: The useable space is reserved_size() - 1 */
    inline std::streamsize reserved_size() const {
      return bufsize;
    }
  private:
   
    inline bool buffer_full() const {
      return len == bufsize;
    }
  
    char* buffer;
    /** 
     * points to the head of the queue. 
     * Reader reads from here
     */
    std::streamsize head;  
  
    /** 
     * points to one past the end of the queue. 
     * writer writes to here. if tail == head, buffer must be empty
     */
    std::streamsize tail;  
    std::streamsize bufsize; // current size of the buffer
    std::streamsize len;  // number of bytes stored in the buffer
  };

  /**
     A boost source device which can attach to a circular buffer
  */
  struct circular_char_buffer_source {
    circular_char_buffer_source(circular_char_buffer &buf, 
                                size_t maxlen = size_t(-1)):buf(buf), maxlen(maxlen) { }
  
    circular_char_buffer &buf;
    size_t maxlen;
    typedef char        char_type;
    struct category : public boost::iostreams::source_tag { };

    /** to satisfy the optimally buffered tag. Since this is an
        in-memory buffer. the optimal buffer size (for any wrapping 
        stream) is 0. */
    inline std::streamsize optimal_buffer_size() const { return 0; }

    inline std::streamsize read(char* s, std::streamsize n) {
      if ((size_t)(n) > maxlen) n = (std::streamsize)maxlen;
      maxlen -= (size_t)n;
      if (n == 0) return -1;
      else return buf.read(s, n);
    }
  };

  /**
     A boost sink device which can attach to a circular buffer
  */
  struct circular_char_buffer_sink {
    circular_char_buffer_sink(circular_char_buffer &buf):buf(buf) { }
  
    circular_char_buffer &buf;
    typedef char        char_type;
    struct category: public boost::iostreams::sink_tag,
                     public boost::iostreams::multichar_tag { };

    /** to satisfy the optimally buffered tag. Since this is an
        in-memory buffer. the optimal buffer size is 0. */
    inline std::streamsize optimal_buffer_size() const { return 0; }

    inline std::streamsize write(const char* s, std::streamsize n) {
      return buf.write(s, n);
    }
  };

  struct circular_char_buffer_device {
    circular_char_buffer_device(circular_char_buffer &buf):buf(buf) { }
  
    circular_char_buffer &buf;
    typedef char      char_type;
    struct category : public boost::iostreams::bidirectional_device_tag,
                      public boost::iostreams::optimally_buffered_tag{ };

    /** to satisfy the optimally buffered tag. Since this is an
        in-memory buffer. the optimal buffer size is 0. */
    inline std::streamsize optimal_buffer_size() const { return 0; }

    inline std::streamsize write(const char* s, std::streamsize n) {
      return buf.write(s, n);
    }
  
    inline std::streamsize read(char* s, std::streamsize n) {
      return buf.read(s, n);
    }
  };

}
#endif

