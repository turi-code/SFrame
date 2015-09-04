/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_SPARSE_ROW_STORAGE_H_
#define GRAPHLAB_SPARSE_ROW_STORAGE_H_

#include <vector>
#include <type_traits>
#include <logger/assertions.hpp>
#include <serialization/serialization_includes.hpp>
#include <boost/iterator/iterator_facade.hpp>

namespace graphlab {

////////////////////////////////////////////////////////////////////////////////

#ifdef NDEBUG
//  ---- RELEASE MODE ---
// also enable all the inline and flatten attributes
#define SRS_ALWAYS_INLINE inline __attribute__((always_inline))
#define SRS_ALWAYS_INLINE_FLATTEN inline __attribute__((always_inline,flatten))
#else
//  ---- DEBUG MODE ---
// disable the always_inline and flatten attributes
#define SRS_ALWAYS_INLINE inline
#define SRS_ALWAYS_INLINE_FLATTEN inline
#endif

/**
 *  \brief A value that can be used as an entry into the
 *  simple_row_storage container in order to hold index information
 *  along with the value.
 *
 *  To use this as an index, specify it as the template parameter to
 *  the simple_row_storage container, e.g. as
 *  simple_row_storage<indexed_entry<double> >.
 *
 *  Note: This doesn't enable a sparse matrix; for that, use Eigen.
 *
 *  \tparam T The type stored in the simple_row_storage container.
 */
template <typename D>
class indexed_entry {
 public:
  /// Construct the indexed_entry from and index and value pair.
  SRS_ALWAYS_INLINE_FLATTEN indexed_entry(size_t __index, const D& __value)
      : _index(__index)
      , _value(__value)
  {}

  indexed_entry() {}

  /// The index associated with the entry.
  SRS_ALWAYS_INLINE_FLATTEN size_t index() const { return _index; }

  /// The value of the entry.
  SRS_ALWAYS_INLINE_FLATTEN const D& value() const {return _value; }

  /// Serialize to stream
  void save(graphlab::oarchive& oarc) const {
    oarc << _index << _value;
  }

  /// Deserialize from stream
  void load(graphlab::iarchive& iarc) {
    iarc >> _index >> _value;
  }

  /// Test equality
  inline bool operator==(const indexed_entry& other) const {
    return (_index == other._index) && (_value == other._value);
  }

  /// Test equality
  inline bool operator!=(const indexed_entry& other) const {
    return (_index != other._index) || (_value != other._value);
  }

 private:
  size_t _index;
  D _value;
};

template <typename T> class simple_row_storage;
template <typename T, typename RowRef> class _simple_row_storage_iterator;

/**
 *  \brief The internal data point class used to reference the data in
 *  the sparse storage container.
 *
 *  This can essentially be thought of as an immutable vector.
 *
 *  Note: if the original sparse matrix is destroyed or modified, all
 *  row_references become invalid.
 *
 *  \tparam T The type stored in the simple_row_storage
 */
template <typename T>
class row_reference {
 public:

  // Do these to match vector.  This behaves just like a simple
  // std::vector that cannot be changed.

  /// The non-const value type referenced.
  typedef T value_type;

  /// The const value type referenced.
  typedef const T const_value_type;

  /// The type of the value reference.
  typedef T& reference;

  /// The type of the const_reference.
  typedef const T& const_reference;

  /// The type of the iterator.
  typedef T* iterator;

  /// The type of the const_iterator.
  typedef const T* const_iterator;

  /// Empty constructor
  row_reference()
      : _begin(nullptr), _end(nullptr)
  {}

  /// Construct with the iterators bounding thing
  row_reference(iterator __begin, iterator __end)
      : _begin(__begin), _end(__end)
  {}

  /// The number of elements referenced.
  SRS_ALWAYS_INLINE_FLATTEN
  size_t size() const                        { return _end - _begin; }

  /// The beginning iterator.
  SRS_ALWAYS_INLINE_FLATTEN
  const_iterator begin() const               { return _begin; }

  /// \overload
  SRS_ALWAYS_INLINE_FLATTEN
  iterator begin()                           { return _begin; }

  /// The ending iterator.
  SRS_ALWAYS_INLINE_FLATTEN
  const_iterator end() const                 { return _end; }

  /// \overload
  SRS_ALWAYS_INLINE_FLATTEN
  iterator end()                             { return _end; }

  /// Returns a reference to an individual element.
  SRS_ALWAYS_INLINE_FLATTEN
  const_reference operator[](size_t i) const { return at(i); }

  /// Returns a const reference to an individual element.
  SRS_ALWAYS_INLINE_FLATTEN
  reference operator[](size_t i)             { return at(i); }

  /// Returns a const reference to an individual element.
  SRS_ALWAYS_INLINE_FLATTEN
  const_reference at(size_t i) const         { return *(_begin + i); }

  /// Returns a reference to an individual element.
  SRS_ALWAYS_INLINE_FLATTEN
  reference at(size_t i)                     { return *(_begin + i); }

  /// Returns true if the row is empty.
  SRS_ALWAYS_INLINE_FLATTEN
  bool empty() const                         { return _begin == _end; }

  /// Returns a reference to the front of the row
  SRS_ALWAYS_INLINE_FLATTEN
  const_reference front() const {
    DASSERT_FALSE(empty());
    return *_begin;
  }

  /// \overload
  SRS_ALWAYS_INLINE_FLATTEN
  reference front() {
    DASSERT_FALSE(empty());
    return *_begin;
  }

  /// Returns a reference to the back of the row
  SRS_ALWAYS_INLINE_FLATTEN
  const_reference back() const {
    DASSERT_FALSE(empty());
    return *(_end - 1);
  }

  /// \overload
  SRS_ALWAYS_INLINE_FLATTEN
  reference back() {
    DASSERT_FALSE(empty());
    return *(_end - 1);
  }

  /// Returns true if this row is the same size as the other row and
  bool operator==(const row_reference& other) const {
    if(&other == this)
      return true;

    if(other.size() != size())
      return false;

    for(size_t i = 0; i < size(); ++i)
      if(at(i) != other.at(i))
        return false;

    return true;
  }

  /// Returns true if this row differs from other
  bool operator!=(const row_reference& other) const {
    return !(*this == other);
  }

 private:
  iterator _begin, _end;
};

////////////////////////////////////////////////////////////////////////////////

/** \brief A simple heterogenous-length row storage container.
 *
 *  A simple, space efficient row storage container.  Supports
 *  efficient adding and retrieval of blocks of data with possibly
 *  varying size.  Can be combined with the indexed_entry class to
 *  include row index information.  Once inserted in the container,
 *  the data is immutable.
 *
 *  Data may be retrieved by row using data[i], or iteration is
 *  supported over the data in the table.  Rows are accessed using the
 *  row_reference class, returned from operator[] or the row(index)
 *  methods.
 *
 *  Currently, building the data structure is supported only by
 *  sequentially adding rows using add(...); however, other methods of
 *  filling this container will be supported in the future as needs
 *  require.
 *
 *  \tparam T the type of the data value stored.
 */
template <typename T>
class simple_row_storage {
 public:

  /// The type of the stored data value.
  typedef T value_type;

  /// The type of the stored data value (const).
  typedef const T const_value_type;

  /// The type of the row reference class (const case)
  typedef row_reference<value_type> row_reference_type;

  /// The type of the row reference class (const case)
  typedef const row_reference<const value_type> const_row_reference_type;

  /// The type of the row reference class. (For compatability with stl.)
  typedef row_reference<value_type> reference;

  /// The type of the row reference class (const case, for compatability with stl.)
  typedef const row_reference<const value_type> const_reference;

  typedef _simple_row_storage_iterator<value_type, row_reference<value_type> > iterator;
  typedef _simple_row_storage_iterator<const value_type, const row_reference<const value_type> > const_iterator;

  /// Default constructor; constructs an empty map.
  simple_row_storage()
      : point_start_map{0}
  {
  }

  /** Returns the number of data rows in the container.  (See nnz() to
   * get the total number of entries.)
   */
  size_t size() const {
    return point_start_map.size() - 1;
  }

  /// Returns true if the container is empty, and false otherwise.
  bool empty() const {
    return point_start_map.size() == 1;
  }

  /// The number of non-zero elements present in the
  size_t nnz() const {
    return value_storage.size();
  }

  /** Reserve space for s total entries.
   *
   * \param s The number of entries to reserve space for.
   */
  void reserve_nnz(size_t s) {
    value_storage.reserve(s);
  }

  /** Reserve space for s rows.  Note that this is different than
   * reserving for a specified number of nonzero elements.
   *
   * \param s The number of rows to reserve space for.
   */
  void reserve(size_t s) {
    point_start_map.reserve(s);
  }

  /** Clear all data in the container, invalidating all iterators to
   * the data.
   */
  void clear() {
    {
      std::vector<size_t> psm{0};
      point_start_map.swap(psm);
    }

    {
      std::vector<value_type> vt;
      value_storage.swap(vt);
    }
  }

 private:
  template <typename ForwardIterator>
  inline size_t _internal_add(const ForwardIterator& it_start,
                              const ForwardIterator& it_end) {

    DASSERT_EQ(point_start_map.back(), value_storage.size());

    size_t idx = point_start_map.size() - 1;

    value_storage.insert(value_storage.end(), it_start, it_end);

    point_start_map.push_back(value_storage.size());

    return idx;
  }


 public:
  /** Add a block of data into the container as a row.  This is a
   * generic method that adds a block of data by an STL-compatible
   * iterator.
   *
   * \param it_start A starting STL-compatible iterator the data to be copied in. 
   *
   * \param it_end   The ending STL-compatible iterator the data to be copied in. 
   *
   * \tparam ForwardIterator An STL-compatible forward iterator that
   * can be dereferenced to value_type.
   *
   * \return The index of the row.
   *
   */
  template <typename ForwardIterator>
  inline size_t add(const ForwardIterator& it_start,
                    const ForwardIterator& it_end,
                    typename std::enable_if<
                      std::is_convertible<
                        typename ForwardIterator::value_type, value_type>::value>::type* = 0) {
    
    return _internal_add(it_start, it_end); 
  }

  /** Add a block of data into the container as a row.  This is a
   * generic method that adds a block of data by a pair of pointers.
   *
   * \param it_start A starting pointer to the data to be copied in.
   *
   * \param it_end   An ending pointer to the data to be copied in.
   *
   * \tparam CastableDataType A data type that can be converted to value_type.
   *
   * \return The index of the row.
   */
  template <typename CastableDataType>
  inline size_t add(const CastableDataType* ptr_start,
                    const CastableDataType* ptr_end,
                    typename std::enable_if<
                      std::is_convertible<CastableDataType, value_type>::value>::type* = 0) {
    
    return _internal_add(ptr_start, ptr_end); 
  }
  
  /** Add a block of data into the container as a row.
   *
   * \param x A row of the data to be copied in as a row.
   *
   * \tparam CastableDataType A data type that can be converted to value_type.
   *
   * \return The index of the row.
   */
  template <typename CastableDataType>
  inline size_t add(const std::vector<CastableDataType>& x,
                    typename std::enable_if<
                      std::is_convertible<CastableDataType, value_type>::value>::type* = 0) {
    
    return _internal_add(x.begin(), x.end()); 
  }

  /** Add a block of n repetitions of the value v as a row.
   *
   * \param n Size of the row.
   * \param v Value to fill the row with.
   *
   * \return The index of the row.
   */
  inline size_t add(size_t n, const value_type& v) {

    DASSERT_EQ(point_start_map.back(), value_storage.size());

    size_t idx = point_start_map.size() - 1;

    value_storage.insert(value_storage.end(), n, v);

    point_start_map.push_back(value_storage.size());

    return idx;
  }

  /// Alias for row(index).
  SRS_ALWAYS_INLINE_FLATTEN
  const_row_reference_type operator[](size_t index) const {
    return row(index);
  }

  /// Alias for row(index).
  SRS_ALWAYS_INLINE_FLATTEN
  row_reference_type operator[](size_t index) {
    return row(index);
  }

  /** \brief Returns a reference to the row at the given index.
   *
   * Returns a reference to the row at the given index.  While the
   * entries of the row may be modified (assuming non-const-ness), the
   * size of the row cannot be.
   */
  SRS_ALWAYS_INLINE_FLATTEN
  const row_reference<const value_type> row(size_t index) const {
    DASSERT_LT(index, size());
    return row_reference<const_value_type>(&value_storage[point_start_map[index]],
                                           &value_storage[point_start_map[index + 1]]);
  }

  /// \overload
  SRS_ALWAYS_INLINE_FLATTEN
  row_reference<value_type> row(size_t index) {
    DASSERT_LT(index, size());
    return row_reference<value_type>(&value_storage[point_start_map[index]],
                                     &value_storage[point_start_map[index + 1]]);
  }

  /// Returns the size of row index.
  size_t row_size(size_t index) const {
    DASSERT_LT(index, size());
    return point_start_map[index + 1] - point_start_map[index];
  }

  /// Serialize to stream
  void save(graphlab::oarchive& oarc) const {
    oarc << value_storage << point_start_map;
  }

  /// Deserialize from stream
  void load(graphlab::iarchive& iarc) {
    iarc >> value_storage >> point_start_map;
  }

  /// Swap data with another simple_row_storage container.
  void swap(simple_row_storage& other) {
    value_storage.swap(other.value_storage);
    point_start_map.swap(other.point_start_map);
  }

  /// Returns an iterator pointing to the first row
  const_iterator begin() const;

  /// \overload
  iterator begin();

  /// Returns an iterator pointing to the end
  const_iterator end() const;

  /// \overload
  iterator end();

  /// Returns true if all elements in the other container are equal, and false otherwise.
  bool operator==(const simple_row_storage& other) const {
    if(&other == this)
      return true;

    return (size() == other.size()
            && point_start_map == other.point_start_map
            && value_storage == other.value_storage);
  }
  
  bool operator!=(const simple_row_storage& other) const {
    return !(*this == other); 
  }

 private:
  std::vector<value_type> value_storage;
  std::vector<size_t> point_start_map;
};


///////////////////////////////////////
// A simple iterator class over rows.

/**
 * A simple forward iterator class over rows in a simple_row_storage
 * container.  Allows the use of for-each construction over rows.
 *
 * \tparam T The type of the value.
 * \tparam RefType The type of the row reference.
 *
 * \internal
 */
template <typename T, typename RefType>
class _simple_row_storage_iterator
    : public boost::iterator_facade<_simple_row_storage_iterator<T, RefType>,
                                    RefType,
                                    boost::forward_traversal_tag> {
 public:
  _simple_row_storage_iterator()
      : data_ptr(nullptr)
      , row_ptr_data(nullptr)
      , index(0)
  {}

private:
  _simple_row_storage_iterator(T* _data_ptr,
                               const std::vector<size_t>* _row_ptr_data,
                               size_t _index)
      : data_ptr(_data_ptr)
      , row_ptr_data(_row_ptr_data)
      , index(_index)
  {
    _set_current_row();
  }

  friend class boost::iterator_core_access;

  friend class simple_row_storage<const T>;
  friend class simple_row_storage<typename std::remove_const<T>::type>;

  void increment() {
    data_ptr += (row_ptr_data->at(index + 1) - row_ptr_data->at(index));
    ++index;
    _set_current_row();
  }

  bool equal(const _simple_row_storage_iterator& other) const {
    return index == other.index;
  }

  RefType& dereference() const {
    return this->current_row;
  }

  inline void _set_current_row() {
    if(index < (row_ptr_data->size() - 1) ) {
      size_t row_size = row_ptr_data->at(index + 1) - row_ptr_data->at(index);
      current_row = row_reference<T>(data_ptr, data_ptr + row_size);
    } else {
      current_row = row_reference<T>();
    }
  }

  T* data_ptr;
  const std::vector<size_t>* row_ptr_data;
  size_t index;

  // This is mutable as dereference() has to be const
  mutable row_reference<T> current_row;
};


////////////////////////////////////////////////////////////////////////////////
// Implementation of the some of the above items

template <typename T>
typename simple_row_storage<T>::const_iterator
simple_row_storage<T>::begin() const {
  return typename simple_row_storage<T>::const_iterator(
      this->empty() ? nullptr : value_storage.data(),
      &point_start_map,
      0);
}

template <typename T>
typename simple_row_storage<T>::iterator simple_row_storage<T>::begin() {
  return typename simple_row_storage<T>::iterator(
      this->empty() ? nullptr : value_storage.data(),
      &point_start_map,
      0);
}

template <typename T>
typename simple_row_storage<T>::const_iterator
simple_row_storage<T>::end() const {
  return typename simple_row_storage<T>::const_iterator(
      this->empty() ? nullptr : value_storage.data(),
      &point_start_map,
      this->size());
}

template <typename T>
typename simple_row_storage<T>::iterator simple_row_storage<T>::end() {
  return typename simple_row_storage<T>::iterator(
      this->empty() ? nullptr : value_storage.data(),
      &point_start_map,
      this->size());
}

}

////////////////////////////////////////////////////////////////////////////////
// Operators to allow nice easy printing

/// Enables easy printing of the row of a simple_row_storage.
template <typename T>
std::basic_ostream<char>& operator<<(std::basic_ostream<char>& out, const graphlab::row_reference<T>& x) {
  out << "{size=" << x.size() << "} (";

  if(x.size() != 0) {
    for(size_t i = 0; i < x.size() - 1; ++i)
      out << x[i] << ", ";

    out << x[x.size() - 1];
  }

  out << ")";

  return out;
}

#endif /* GRAPHLAB_SPARSE_ROW_STORAGE_H_ */
