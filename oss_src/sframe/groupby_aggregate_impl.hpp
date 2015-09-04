/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_SFRAME_GROUPBY_AGGREGATE_IMPL_HPP
#define GRAPHLAB_SFRAME_GROUPBY_AGGREGATE_IMPL_HPP

#include <memory>
#include <vector>
#include <cstdint>
#include <functional>
#include <unordered_set>
#include <sframe/sframe.hpp>
#include <util/cityhash_gl.hpp>
#include <parallel/mutex.hpp>
#include <sframe/group_aggregate_value.hpp>
#include <graphlab/util/hopscotch_map.hpp>

namespace graphlab {
namespace groupby_aggregate_impl {
/**
 * A description of a group operation.
 */
struct group_descriptor {
  /// The column number of operate on
  std::vector<size_t> column_numbers;
  /// The aggregator
  std::shared_ptr<group_aggregate_value> aggregator;
};



/**
 * This class manages all the intermedate aggregation result of a given key.
 * It contains a key, and an array of multiple aggregated values for that key
 * (each aggregated value is for a different aggregator. For instance, one
 * could be sum, one could be count).
 * It then provides
 */
struct groupby_element {

  /// The key of this aggregation
  std::vector<flexible_type> key;
  /// All the aggregated values
  mutable std::vector<std::unique_ptr<group_aggregate_value> > values;

  /// A cache of the hash of the key
  size_t hash_val;

  groupby_element() = default;

  /**
   * Constructs a group element from a key, and a description of all
   * the group operations. All the aggregated values will be initialized as
   * new empty values.
   */
  groupby_element(std::vector<flexible_type> group_key,
                  const std::vector<group_descriptor>& group_desc);

  /**
   * Constructs a groupby_element from a string which contains a
   * serialization of the element. The array of all the descriptors is
   * required.
   */
  groupby_element(const std::string& val,
                  const std::vector<group_descriptor>& group_desc);

  /**
   * Constructs a group element from a key, and a description of all
   * the group operations. All the aggregated values will be initialized as
   * new empty values.
   */
  void init(std::vector<flexible_type> group_key,
            const std::vector<group_descriptor>& group_desc);


  /// Writes the group result into an output archive
  void save(oarchive& oarc) const;

  /**
   * Loads the group result from an input archive and a group
   * operation descriptor
   */
  void load(iarchive& iarc,
            const std::vector<group_descriptor>& group_desc);

  /**
   * Provides a total ordering on group-by elements
   */
  bool operator>(const groupby_element& other) const;

  /**
   * Provides a total ordering on group-by elements
   */
  bool operator<(const groupby_element& other) const;

  /**
   * Returns true if this, and other have identical keys
   */
  bool operator==(const groupby_element& other) const;

  /**
   * Combines values another groupby element which is performing the
   * same set of operations
   */
  void operator+=(const groupby_element& other);

  template <typename T>
  void add_element(const T& val,
                   const std::vector<group_descriptor>& group_desc) const;

  static size_t hash_key(const std::vector<flexible_type>& key);

  static size_t hash_key(const std::vector<flexible_type>& key, size_t keylen);

  static size_t hash_key(const sframe_rows::row& key);

  static size_t hash_key(const sframe_rows::row& key, size_t keylen);

  size_t hash() const;

  void compute_hash();
};


} // namespace grouby_aggregate_impl
} // namespace graphlab

// we need to put the hash struct in std
namespace std {
/**
 * Hash function.
 *
 * This allows us to add groupby_element to an std::unordered_set
 */
template<>
struct hash<graphlab::groupby_aggregate_impl::groupby_element> {
  size_t operator()(
      const graphlab::groupby_aggregate_impl::groupby_element& element) const {
    return element.hash();
  }
};
} // namespace std




namespace graphlab {
namespace groupby_aggregate_impl {

/**
 * This
 */
class group_aggregate_container {

 public:
   /// construct with given sarray and the segmentid as sink.
   group_aggregate_container(size_t max_buffer_size,
                             size_t num_segments);

   /// Deleted copy constructor
   group_aggregate_container(const group_aggregate_container& other) = delete;

   /// Deleted assignment operator
   group_aggregate_container&
       operator=(const group_aggregate_container& other) = delete;

   /**
    * Adds a new group operation which groups the values of a column
    */
   void define_group(std::vector<size_t> column_numbers,
                     std::shared_ptr<group_aggregate_value> aggregator);

   /// Add a new element to the container.
   void add(const std::vector<flexible_type>& val,
            size_t num_keys);

   /// Add a new element to the container.
  void add(const sframe_rows::row& val,
            size_t num_keys);

   /// Sort all elements in the container and writes to the output.
   void group_and_write(sframe& out);
  private:

   /// collection of all the group operations
   std::vector<group_descriptor> group_descriptors;

   struct segment_information {
     /// Locks on the elements structure
     graphlab::simple_spinlock in_memory_group_lock;
     graphlab::simple_spinlock fine_grain_locks[128];
     atomic<size_t> refctr;
     /// Intermediate group values
     hopscotch_map<size_t, std::vector<groupby_element>* > elements;

     /// Locks on the below structures
     graphlab::mutex file_lock;
     /// The temporary storage for the grouped values
     sarray<std::string>::iterator outiter;
     /// Storing the size of each sorted chunk.
     std::vector<size_t> chunk_size;
   };

   /// Writes the content into the sarray segment backend.
   void flush_segment(size_t segmentid);

   size_t max_buffer_size;
   std::vector<segment_information> segments;
   sarray<std::string> intermediate_buffer;
   std::unique_ptr<sarray<std::string>::reader_type> reader;

   /// Sort all elements in the container and writes to the output.
   void group_and_write_segment(sframe& out,
                                std::shared_ptr<sarray<std::string>::reader_type> reader,
                                size_t segmentid);
};


} // namespace groupby_aggregate_impl
} // namespace graphlab


#endif //  GRAPHLAB_SFRAME_GROUPBY_AGGREGATE_IMPL_HPP
