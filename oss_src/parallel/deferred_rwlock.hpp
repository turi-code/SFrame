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


#ifndef DEFERRED_RWLOCK_HPP
#define DEFERRED_RWLOCK_HPP
#include <parallel/pthread_tools.hpp>
#include <parallel/queued_rwlock.hpp>
#include <logger/assertions.hpp>
namespace graphlab {
class deferred_rwlock{
 public:

  struct request{
    char lockclass : 2;
    __attribute__((may_alias)) uint64_t id : 62; 
    request* next;
  };
 private:
  request* head;
  request* tail;
  uint16_t reader_count;
  bool writer;
  simple_spinlock lock;
 public:

  deferred_rwlock(): head(NULL),
                      tail(NULL), reader_count(0),writer(false) { }

  // debugging purposes only
  inline size_t get_reader_count() {
    __sync_synchronize();
    return reader_count;
  }

  // debugging purposes only
  inline bool has_waiters() {
    return head != NULL || tail != NULL;
  }

  inline void insert_queue(request *I) {
    if (head == NULL) {
      head = I;
      tail = I;
    }
    else {
      tail->next = I;
      tail = I;
    }
  }
  inline void insert_queue_head(request *I) {
    if (head == NULL) {
      head = I;
      tail = I;
    }
    else {
      I->next = head;
      head = I;
    }
  }
  
  inline bool writelock_priority(request *I) {
    I->next = NULL;
    I->lockclass = QUEUED_RW_LOCK_REQUEST_WRITE;
    lock.lock();
    if (reader_count == 0 && writer == false) {
      // fastpath
      writer = true;
      lock.unlock();
      return true;
    }
    else {
      insert_queue_head(I);
      lock.unlock();
      return false;
    }
  }
  
  inline bool writelock(request *I) {
    I->next = NULL;
    I->lockclass = QUEUED_RW_LOCK_REQUEST_WRITE;
    lock.lock();
    if (reader_count == 0 && writer == false) {
      // fastpath
      writer = true;
      lock.unlock();
      return true;
    }
    else {
      insert_queue(I);
      lock.unlock();
      return false;
    }
  }

  // completes the write lock on the head. lock must be acquired
  // head must be a write lock
  inline void complete_wrlock() {
  //  ASSERT_EQ(reader_count.value, 0);
    head = head->next;
    if (head == NULL) tail = NULL;
    writer = true;
  }

  // completes the read lock on the head. lock must be acquired
  // head must be a read lock
  inline size_t complete_rdlock(request* &released) {
    released = head;
    size_t numcompleted = 1;
    head = head->next;
    request* readertail = released;
    while (head != NULL && head->lockclass == QUEUED_RW_LOCK_REQUEST_READ) {
      readertail = head;
      head = head->next;
      numcompleted++;
    }
    reader_count += numcompleted;
    if (head == NULL) tail = NULL;
    
    // now released is the head to a reader list
    // and head is the head of a writer list
    // I want to go through the writer list and extract all the readers
    // this essentially 
    // splits the list into two sections, one containing only readers, and 
    // one containing only writers.
    // (reader biased locking)
    if (head != NULL) {
      request* latestwriter = head;
      request* cur = head->next;
      while (1) {
        if (cur->lockclass == QUEUED_RW_LOCK_REQUEST_WRITE) {
          latestwriter = cur;
        }
        else {
          readertail->next = cur;
          readertail = cur;
          reader_count++;
          numcompleted++;
          latestwriter->next = cur->next;
        }
        if (cur == tail) break;
        cur=cur->next;
      }
    }
    return numcompleted;
  }
  
  inline size_t wrunlock(request* &released) {
    released = NULL;
    lock.lock();
    writer = false;
    size_t ret = 0;
    if (head != NULL) {
      if (head->lockclass == QUEUED_RW_LOCK_REQUEST_READ) {
        ret = complete_rdlock(released);
        if (ret == 2) assert(released->next != NULL);
      }
      else {
        writer = true;
        released = head;
        complete_wrlock();
        ret = 1;
      }
    }
    lock.unlock();
    return ret;
  }

  inline size_t readlock(request *I, request* &released)  {
    released = NULL;
    size_t ret = 0;
    I->next = NULL;
    I->lockclass = QUEUED_RW_LOCK_REQUEST_READ;
    lock.lock();
    // there are readers and no one is writing
    if (head == NULL && writer == false) {
      // fast path
      ++reader_count;
      lock.unlock();
      released = I;
      return 1;
    }
    else {
      // slow path. Insert into queue
      insert_queue(I);
      if (head->lockclass == QUEUED_RW_LOCK_REQUEST_READ && writer == false) {
        ret = complete_rdlock(released);
      }
      lock.unlock();
      return ret;
    }
  }

  inline size_t readlock_priority(request *I, request* &released)  {
    released = NULL;
    size_t ret = 0;
    I->next = NULL;
    I->lockclass = QUEUED_RW_LOCK_REQUEST_READ;
    lock.lock();
    // there are readers and no one is writing
    if (head == NULL && writer == false) {
      // fast path
      ++reader_count;
      lock.unlock();
      released = I;
      return 1;
    }
    else {
      // slow path. Insert into queue
      insert_queue_head(I);
      if (head->lockclass == QUEUED_RW_LOCK_REQUEST_READ && writer == false) {
        ret = complete_rdlock(released);
      }
      lock.unlock();
      return ret;
    }
  }

  inline size_t rdunlock(request* &released)  {
    released = NULL;
    lock.lock();
    --reader_count;
    if (reader_count == 0) {
      size_t ret = 0;
      if (head != NULL) {
        if (head->lockclass == QUEUED_RW_LOCK_REQUEST_READ) {
          ret = complete_rdlock(released);
        }
        else {
          writer = true;
          released = head;
          complete_wrlock();
          ret = 1;
        }
      }
      lock.unlock();
      return ret;
    }
    else {
      lock.unlock();
      return 0;
    }
  }
};

}
#endif

