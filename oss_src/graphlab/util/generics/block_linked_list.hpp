/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
/*  
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


#ifndef GRAPHLAB_BLOCK_LINK_LIST_HPP
#define GRAPHLAB_BLOCK_LINK_LIST_HPP
#include <graphlab/util/generics/dynamic_block.hpp>
#include <logger/assertions.hpp>
#include <boost/iterator/iterator_facade.hpp>
#include <boost/iterator/iterator_adaptor.hpp>
#include <boost/type_traits/is_convertible.hpp>
#include <boost/utility/enable_if.hpp>

#include <stdint.h>
#include <algorithm>

namespace graphlab {
  template<typename valuetype, uint32_t blocksize=(4096-20)/sizeof(valuetype)>
  /**
   * This class represents a forward linked list of dynamic block.
   */
  class block_linked_list {
   public:
     typedef dynamic_block<valuetype, blocksize> blocktype;

   //////////////////// Constructors ///////////////////////// 
   public:
     /// Construct empty list
     block_linked_list() : head(NULL), tail(NULL), _size(0) { }

     /// Construct list from container 
     template<typename InputIterator>
     block_linked_list(InputIterator first, InputIterator last) { 
       if (first == last) 
         return;
       assign(first, last);
     }

     /// Destructor
     ~block_linked_list() { clear(); }

     /**
      * Assign the list with values from given iterator pair. 
      */
     template<typename InputIterator>
     void assign(InputIterator first, InputIterator last)  {

       size_t new_size = last - first;
       if (new_size == 0)
         return;

       if (_size > 0)
         clear();

       InputIterator iter = first;

       blocktype* current = head;
       // if the list is empty, create the head block.
       if (current == NULL) {
         current = head = tail = new blocktype();
       }

       while (iter != last) {
         InputIterator end =  std::min(iter+blocksize, last);
         current->assign(iter, end);
         iter = end;
         if (iter != last) {
           current = new blocktype();
           tail->_next = current;
           tail = current;
         }
       }
       _size = new_size; 
     }

     /// Returns the size of the list
     size_t size() const { return _size; }

     static const size_t get_blocksize() {
       return blocksize;
     }

   //////////////////// Iterator API ///////////////////////// 
   public:
     /**
      * Defines the iterator of the values stored in the list.
      * The iterator has random_access_traversal_tag, however
      * the actual random access takes O(n/blocksize);
      */
     template <typename value>
     class value_iterator :
         public boost::iterator_facade<value_iterator<value>, value,
                                       boost::random_access_traversal_tag> {
     private:
        struct enabler {};

     public:
        value_iterator(blocktype* blockptr, uint32_t offset) : 
            blockptr(blockptr), offset(offset) { }

        template <typename othervalue>
            value_iterator(value_iterator<othervalue> const& other,
                        typename boost::enable_if<
                        boost::is_convertible<othervalue*,value*>,
                        enabler>::type = enabler()) 
            : blockptr(other.blockptr), offset(other.offset) { }


     public:
        // public wrapper of the private distance_to
      ptrdiff_t pdistance_to(const value_iterator& other) const {
        return distance_to(other);
      }

      // core access functions
      private:         
        friend class boost::iterator_core_access;
        template <class> friend class value_iterator;

        /////////////////  Forward traversal core functions ///////////////
        void increment() { 
          if (offset < blockptr->size()-1) {
            ++offset;
          } else {
            blockptr = blockptr->_next;
            offset = 0;
          }
        }

        template <typename othervalue>
        bool equal(value_iterator<othervalue> const& other) const {
          return (blockptr == other.blockptr) && (offset == other.offset);
        }

        value& dereference() const { 
          return blockptr->values[offset];
        }

        /////////////////  Random access core functions ///////////////
        void advance(int n) {
          size_t dist = n+offset;
          while(dist >= blockptr->size() && blockptr != NULL) {
            dist -= blockptr->size();
            blockptr = blockptr->next();
          } 
          if (blockptr == NULL) {
            offset = 0;
          } else {
            offset = dist;
          }
        }

        ptrdiff_t distance_to(const value_iterator& other) const {
          ptrdiff_t dist = 0;
          if (blockptr == other.blockptr) {
            return ((ptrdiff_t)other.offset - offset);
          } else {
            blocktype* cur = blockptr;
            // try moving forward until we hit other or NULL
            while (cur != NULL && cur != other.blockptr) {
              dist += cur->size();
              cur = cur->next();
            }

            // this catched other
            if (cur != NULL || other.blockptr == NULL) {
              return (dist + (ptrdiff_t)other.offset - offset);
            } else {
              logstream(LOG_INFO)<< "block list iterator reverse direction!!" << std::endl; 
              // this hit the dead end, need to move backwards 
              dist = 0;
              cur = other.blockptr;
              while (cur != NULL && cur != blockptr) {
                dist += cur->size();
                cur = cur->next();
              }
              return -(dist + (ptrdiff_t)offset - other.offset);
            }
          }
        }

     // For internal use only. Get access to the internal block pointer and offset.
     public: 
        // returns block pinter 
        blocktype*& get_blockptr() {
          return blockptr;
        }
        uint32_t& get_offset() {
          return offset;
        }

      private:
        blocktype* blockptr;
        uint32_t offset;
     }; // end of value iterator

     typedef value_iterator<valuetype> iterator; 
     typedef value_iterator<valuetype const> const_iterator; 

     iterator begin() {
       return iterator(head, 0);
     }

     iterator end() {
       return iterator(NULL, 0);
     }

     const_iterator begin() const {
       return const_iterator(head, 0);
     }

     const_iterator end() const {
       return const_iterator(NULL, 0);
     }

   //////////////////// Insertion API ///////////////////////// 
   /*
    * Insert value into the location of iter.
    * Split the block when necessary.
    * Returns iterator to the new value.
    */
   iterator insert(iterator iter, const valuetype& val) {
     iterator ins_iter = get_insert_iterator(iter);
     blocktype* ins_ptr = ins_iter.get_blockptr();
     uint32_t offset = ins_iter.get_offset();
     ASSERT_TRUE(ins_ptr != NULL);
     if (ins_ptr->is_full()) {
       // split the block
       ins_ptr->split();
       if(ins_ptr == tail) {
         // update tail pointer
         tail = ins_ptr->next();
       }
       if (offset >= blocksize/2) {
         ins_ptr = ins_ptr->next();
         offset -= (blocksize/2);
       }
     }
     ins_ptr->insert(val,offset);
     ++_size;
     return iterator(ins_ptr, offset);
   }
   
   /**
    * Insert a range of values into the position of the given iterator.
    * Will create new blocks after the given block when necessary.
    *
    * Returns the begin and end iterator to the new elements. 
    *
    * \note 
    * This operation will NOT affect the blocks after the given block.
    * |x1,x2,y1,y2,y3,y4 , _ , _ , _ , _| -> | ... | -> |...|
    *        ^ 
    *        p
    * blocksize = 10
    * iterator: blockptr = p, offset = 2
    */
   template<typename InputIterator>
   std::pair<iterator, iterator> 
     insert(iterator iter, InputIterator first, InputIterator last) {

     typedef std::pair<iterator, iterator> ret_type;
     const size_t len = last - first;
     if (len == 0) return ret_type(iter, iter);

     iterator ins_iter = get_insert_iterator(iter);

     // Pointers to the block of the insertion point.
     blocktype* ibegin_ptr = ins_iter.get_blockptr(); 
     // number of elements before the insertion point in the block 
     size_t nx = ins_iter.get_offset();
     // number of elements after the insertion point in the block 
     size_t ny = ibegin_ptr->size()-nx;

     // save ys 
     valuetype* swap = (valuetype*)malloc((ny)*sizeof(valuetype));
     memcpy(swap, &(ibegin_ptr->values[nx]), ny*sizeof(valuetype)) ;

     // remove y temporarily
     ibegin_ptr->_size -= ny;
     _size -= ny;

     // Insert new elements to the end of the block,
     // record the begin and end iterators
     ret_type iter_pair = append_to_block(ibegin_ptr, first, last);

     iterator begin_ins_iter = iter_pair.first;
     iterator end_ins_iter = iter_pair.second;
     // the end iterator returned by append_to_block may need adjustment
     if (end_ins_iter.get_offset() == end_ins_iter.get_blockptr()->size()) {
       end_ins_iter.get_blockptr() = end_ins_iter.get_blockptr()->next();
       end_ins_iter.get_offset() = 0;
     }

     // add y back 
     if (ny > 0) {
       blocktype* iend_ptr = iter_pair.second.get_blockptr();
       ret_type iter_pair2 = append_to_block(iend_ptr, swap, swap+ny); 
       end_ins_iter = iter_pair2.first;
     }

     return ret_type(begin_ins_iter, end_ins_iter);
   }

   /*
    * Move up the content of the next block to the end of this block. 
    * Delete the next block if it becomes empty. 
    */
   void merge_next(blocktype* bptr) {
     if (bptr == NULL || bptr->next() == NULL || bptr->is_full()) {
       return;
     }
     blocktype* nextptr = bptr->next();
     size_t spaceleft = blocksize - bptr->size(); // num space left
     size_t nnext = nextptr->size(); // num elems in the next block
     size_t nmove = std::min(spaceleft, nnext); // num elems to move

     // move up nmove elements
     std::copy(nextptr->values, nextptr->values+nmove,
               bptr->values + bptr->_size);
     if (nnext > nmove) {
       valuetype* p = nextptr->values;
       for (size_t i = 0; i < (nnext-nmove); ++i) {
         *(p+i) = *(p+i+nmove);
       }
     }

     // update the size
     bptr->_size += nmove; 
     nextptr->_size -= nmove;

     // remove the next block if empty
     if (nextptr->size() == 0) {
       bptr->_next = nextptr->next();
       if (nextptr == tail) {
         tail = bptr;
       }
       delete nextptr;
     }
   }

   /// Repack the space for a given key 
   void repack(iterator begin_iter, iterator end_iter) {
     blocktype* bptr = begin_iter.get_blockptr();
     blocktype* eptr = end_iter.get_blockptr();

     if (bptr == NULL || bptr == eptr) {
       return;
     }
     while (bptr != eptr && bptr->next() != eptr) {
       merge_next(bptr);
       if (bptr->is_full())  {
         bptr = bptr->next();
       }
     }
    }

   //////////////////// Block Access API ///////////////////////// 
   /*
    * Returns the nth block in the list. Linear time.
    */
   blocktype* get_block(size_t n) {
     size_t i = 0;
     blocktype* cur = head;
     while (cur != NULL && i < n) {
       cur = cur->_next;
       ++i;
     }
     return cur;
   }

   size_t num_blocks() const {
     if (head == NULL) 
       return 0;
     size_t ret = 1;
     blocktype* ptr = head;
     while (ptr != tail) {
       ptr = ptr->next();
       ++ret;
     }
     return ret;
   }

   //////////////////// Pretty print API ///////////////////////// 
   void print(std::ostream& out) const {
     blocktype* cur = head;
     while (cur != NULL) {
       cur->print(out);
       out << "-> ";
       cur = cur->_next;
     }
     out << "||" << std::endl;
   }
   
   //////////////////// Read Write ///////////////////////// 
     void swap (block_linked_list& other) {
       clear();
       delete head;
       head = other.head;
       tail = other.tail;
       _size = other._size;
       other.head = other.tail = NULL;
       other._size = 0;
     }

     void clear() {
       blocktype* tmp = NULL;
       while(head != tail) {
         ASSERT_TRUE(head != NULL);
         tmp = head;
         head = head->_next;
         delete tmp;
       }
       delete head;
       head = tail = NULL;
       _size = 0;
     }


     void save(oarchive& oarc) const {
       serialize_iterator(oarc, begin(), end(), size());
     }
   

   //////////////////// Helper Function ///////////////////////// 
   private:
   /**
    * \internal
    * Allocate new space for insertion if the iterator is end.
    * Return the iterator to the new space.
    */
   iterator get_insert_iterator(iterator iter) {
     bool is_end = (iter == end());
     if (is_end) {
       if (tail == NULL) {
         head = tail = new blocktype();
       } else if (tail->is_full()) {
         append_block();
       }
       iter.get_blockptr() = tail;
       iter.get_offset() = tail->size();
     }
     return iter;
   }

   blocktype* insert_block(blocktype* ins) {
     blocktype* ret = new blocktype();
     ret->_next = ins->next();
     ins->_next = ret;
     if (ins == tail) {
       tail = ret;
     }
     return ret;
   }

   blocktype* append_block() {
     return insert_block(tail);
   }

   /**
    * \internal 
    * Insert a range of values into the end of the given block.
    * Will create new blocks after the given block when necessary.
    *
    * \note 
    * This operation will NOT affect the blocks after the given block.
    *
    * Returns the begin and end iterator to the new elements. 
    *
    * \note 
    * The end iterator does jump to the next block. Instead, it points at the 
    * memory of last_block->size().
    */
  template<typename InputIterator>
  std::pair<iterator, iterator> 
     append_to_block(blocktype* ibegin_ptr, InputIterator first, InputIterator last) {
     ASSERT_TRUE(ibegin_ptr != NULL);
     const size_t len = last-first;

     /// If nothing to append, return the begin location 
     if (len == 0) { 
       iterator ret(ibegin_ptr, ibegin_ptr->size());
       return std::pair<iterator, iterator>(ret, ret);
     }

     /// elements to return
     blocktype* iend_ptr = ibegin_ptr;
     uint32_t ibegin_offset, iend_offset;

     // create a new block if the current is full
     if (ibegin_ptr->is_full()) {
       ibegin_ptr = insert_block(ibegin_ptr); 
     }

     /// Fill in the rest of the current block
     size_t nold = ibegin_ptr->size(); // num of old elements
     size_t spaceleft = (blocksize - nold);  // room left
     size_t nnew = std::min(len, spaceleft); // num of new elements to insert
     ASSERT_TRUE(nold+nnew <= blocksize);
     ASSERT_TRUE(nnew > 0);
     std::copy(first, first+nnew, &(ibegin_ptr->values[nold]));
     ibegin_ptr->_size += nnew;
     ibegin_offset = nold;
     iend_ptr = ibegin_ptr;
     iend_offset = ibegin_ptr->size();

     // Creates a block chain for remaining elements
     if (len > spaceleft) {
       blocktype* current = insert_block(ibegin_ptr); 
       InputIterator iter = first + spaceleft; 
       while (iter != last) {
         InputIterator end =  std::min(iter+blocksize, last);
         current->assign(iter, end);
         iter = end;
         if (iter != last) {
           current = insert_block(current); 
         }
       }
       iend_ptr = current;
       iend_offset = current->size(); 
     }
     _size += len;

     return std::pair<iterator,iterator>(
         iterator(ibegin_ptr, ibegin_offset)
         ,iterator(iend_ptr, iend_offset));
   }


  // Disable copy constructor
    block_linked_list(const block_linked_list& that) = delete;
    block_linked_list& operator=(const block_linked_list& that) = delete;


   //////////////////// Private Data Member ///////////////////////// 
   private:
     blocktype* head;
     blocktype* tail;
     size_t _size;
  };
} // end of namespace
#endif

     // } 
     // else {
     //   blocktype* splitblk = insert_block(ibegin_ptr);
     //   size_t nfirst = (nold+nnew)/2; 
     //   size_t nsecond = (nold+nnew-nfirst);
     //   ibegin_ptr->_size = nfirst;
     //   splitblk->_size = nsecond;
     //   if (nold < nfirst) {
     //     size_t padsize = nfirst - nold;
     //     std::copy(first, first+padsize, &(ibegin_ptr->values[nold]));
     //     std::copy(first+padsize, first+nnew, splitblk->values);
     //     ibegin_offset = nold;
     //   } else {
     //     size_t padsize = nold-nfirst;
     //     memcpy(splitblk->values, &(ibegin_ptr->values[nfirst]), padsize*sizeof(valuetype));
     //     std::copy(first, first+nnew, &(splitblk->values[padsize]));
     //     ibegin_ptr = splitblk;
     //     ibegin_offset = padsize;
     //  }
       // if (!add_new_blocks) {
       //    iend_ptr = splitblk;
       //    iend_offset = nsecond;
       // }
     // }

