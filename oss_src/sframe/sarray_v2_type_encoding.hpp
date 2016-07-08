/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_SFRAME_SARRAY_V2_TYPE_ENCODING_HPP
#define GRAPHLAB_SFRAME_SARRAY_V2_TYPE_ENCODING_HPP
#include <flexible_type/flexible_type.hpp>
#include <sframe/sarray_v2_block_types.hpp>
#include <util/dense_bitset.hpp>
#include <sframe/integer_pack.hpp>
namespace graphlab {
namespace v2_block_impl {
using namespace graphlab::integer_pack;

static const size_t MAX_INTEGERS_PER_BLOCK = 128;
static const size_t MAX_DOUBLES_PER_BLOCK = 512;

void encode_number(block_info& info, 
                   oarchive& oarc, 
                   const std::vector<flexible_type>& data);

void decode_number(iarchive& iarc,
                   std::vector<flexible_type>& ret,
                   size_t num_undefined);

void encode_double(block_info& info, 
                   oarchive& oarc, 
                   const std::vector<flexible_type>& data);
void decode_double(iarchive& iarc,
                   std::vector<flexible_type>& ret,
                   size_t num_undefined);
void decode_double_legacy(iarchive& iarc,
                          std::vector<flexible_type>& ret,
                          size_t num_undefined);
/**
 * Decodes a type block. Reads from block_info and a buffer.
 * Returns false on failure. 
 */
bool typed_decode(const block_info& info,
                  char* start, size_t len,
                  std::vector<flexible_type>& ret);

/**
 * Decodes a type block. Reads from block_info and a buffer.
 * Returns false on failure. 
 */
bool typed_decode_stream_callback(const block_info& info,
                                  char* start, size_t len,
                                  std::function<void(flexible_type)> retcallback);

/**
 * Encodes a type block. Serializes data into the output archive
 * and updates the block_info datastructure.
 */
void typed_encode(const std::vector<flexible_type>& data, 
                  block_info& info,
                  oarchive& oarc);






/**
 * Decodes num_elements of numbers, calling the callback for each number.
 */
template <typename Fn> // Fn is a function like void(flexible_type)
static void decode_number_stream(size_t num_elements,
                                 iarchive& iarc,
                                 Fn callback) {
  uint64_t buf[MAX_INTEGERS_PER_BLOCK];
  while(num_elements > 0) {
    size_t buflen = std::min<size_t>(num_elements, MAX_INTEGERS_PER_BLOCK);
    frame_of_reference_decode_128(iarc, buflen, buf);
    for (size_t i = 0;i < buflen; ++i) {
      callback(flexible_type(buf[i]));
    }
    num_elements -= buflen;
  }
}


/**
 * Decodes num_elements of numbers, calling the callback for each number.
 */
template <typename Fn> // Fn is a function like void(flexible_type)
static void decode_double_stream_legacy(size_t num_elements,
                                 iarchive& iarc,
                                 Fn callback) {
  uint64_t buf[MAX_INTEGERS_PER_BLOCK];
  while(num_elements > 0) {
    size_t buflen = std::min<size_t>(num_elements, MAX_INTEGERS_PER_BLOCK);
    frame_of_reference_decode_128(iarc, buflen, buf);
    for (size_t i = 0;i < buflen; ++i) {
      size_t intval = (buf[i] >> 1) | (buf[i] << 63);
      // make a double flexible_type
      flexible_type ret(0.0);
      ret.mutable_get<flex_int>() = intval;
      callback(ret);
    }
    num_elements -= buflen;
  }
}


/**
 * Decodes num_elements of numbers, calling the callback for each number.
 */
template <typename Fn> // Fn is a function like void(flexible_type)
static void decode_double_stream(size_t num_elements,
                                 iarchive& iarc,
                                 Fn callback) {
  // we reserve one character so we can add new encoders as needed in the future
  char reserved = 0;
  iarc.read(&(reserved), sizeof(reserved));
  ASSERT_LT(reserved, 3);
  if (reserved == DOUBLE_RESERVED_FLAGS::LEGACY_ENCODING) {
    decode_double_stream_legacy(num_elements, iarc, callback);
    return;
  } else if (reserved == DOUBLE_RESERVED_FLAGS::INTEGER_ENCODING) {
    decode_number_stream(num_elements, iarc, 
                         [=](const flexible_type& val) {
                           flex_float ret = flex_float(val.get<flex_int>());
                           callback(ret);
                         });
  } 
}


/**
 * Decodes num_elements of strings , calling the callback for each string.
 */
template <typename Fn> // Fn is a function like void(flexible_type)
static void decode_string_stream(size_t num_elements,
                                 iarchive& iarc,
                                 Fn callback) {
  bool use_dictionary_encoding = false;
  std::vector<flexible_type> idx_values;
  idx_values.resize(num_elements, flexible_type(flex_type_enum::INTEGER));
  iarc >> use_dictionary_encoding;
  if (use_dictionary_encoding) {
    uint64_t num_values;
    std::vector<flexible_type> str_values;
    variable_decode(iarc, num_values);
    str_values.resize(num_values);
    for (auto& str: str_values) {
      std::string new_str;
      uint64_t str_len;
      variable_decode(iarc, str_len);
      new_str.resize(str_len);
      iarc.read(&(new_str[0]), str_len);
      str = std::move(new_str);
    }
    decode_number(iarc, idx_values, 0);
    for (size_t i = 0;i < num_elements; ++i) {
      callback(str_values[idx_values[i].get<flex_int>()]);
    }
  } else {
    // get all the lengths
    decode_number(iarc, idx_values, 0);
    flexible_type ret(flex_type_enum::STRING);
    for (size_t i = 0;i < num_elements; ++i) {
      size_t str_len = idx_values[i].get<flex_int>();
      ret.mutable_get<std::string>().resize(str_len);
      iarc.read(&(ret.mutable_get<std::string>()[0]), str_len);
      callback(ret);
    }
  }
}

/**
 * Decodes num_elements of vectors, calling the callback for each string.
 *
 * This is the 2nd generation vector decoder. its use is flagged by
 * turning on the block flag BLOCK_ENCODING_EXTENSION. 
 */
template <typename Fn> // Fn is a function like void(flexible_type)
static void decode_vector_stream(size_t num_elements,
                                 iarchive& iarc,
                                 Fn callback,
                                 bool new_format) {
  // we reserve one character so we can add new encoders as needed in the future
  if (new_format) {
    char reserved = 0;
    iarc.read(&(reserved), sizeof(reserved));
  }
  // decode the length of each vector
  std::vector<flexible_type> lengths(num_elements);
  decode_number(iarc, lengths, 0);
  size_t total_num_values = 0;
  for (const flexible_type& length : lengths) {
    total_num_values += length.get<flex_int>();
  }

  // decode the values
  std::vector<flexible_type> values(total_num_values);
  if (new_format) {
    decode_double(iarc, values, 0);
  } else {
    decode_double_legacy(iarc, values, 0);
  }

  size_t length_ctr = 0;
  size_t value_ctr = 0;
  flexible_type ret(flex_type_enum::VECTOR);
  for (size_t i = 0 ;i < num_elements; ++i) {
    flex_vec& output_vec = ret.mutable_get<flex_vec>();
    // resize this to the appropriate length
    output_vec.resize(lengths[length_ctr].get<flex_int>());
    ++length_ctr;
    // fill in the value
    for(size_t j = 0; j < output_vec.size(); ++j) {
      output_vec[j] = values[value_ctr].get<flex_float>();
      ++value_ctr;
    }
    callback(ret);
  }
}



/**
 * Decodes a collection of flexible_type values. The array must be of 
 * contiguous type, but permitting undefined values.
 *
 * See \ref typed_encode() for details.
 *
 * \note The coding does not store the number of values stored. This is
 * stored in the block_info (block.num_elem)
 */
template <typename Fn> // Fn is a function like void(flexible_type)
static bool typed_decode_stream_callback(const block_info& info,
                                  char* start, size_t len,
                                  Fn callback) {
  if (!(info.flags & IS_FLEXIBLE_TYPE)) {
    logstream(LOG_ERROR) << "Attempting to decode a non-typed block"
                         << std::endl;
    return false;
  }
  graphlab::iarchive iarc(start, len);

  // some basic block properties which will be filled in
  // number of elements
  size_t dsize = info.num_elem;
  // column type
  flex_type_enum column_type;
  // num undefined.
  size_t num_undefined = 0;
  // undefined bitmap mapping out where are the undefined values.
  // only specified if num_undefined > 0
  graphlab::dense_bitset undefined_bitmap;

  char num_types; 
  iarc >> num_types;
  // if it is a multiple type block, we don't perform a type decode
  bool perform_type_decoding = !(info.flags & MULTIPLE_TYPE_BLOCK);
  if (perform_type_decoding) {
    if (num_types == 0) {
      // empty block
      return true;
    } else if (num_types == 1) {
      //  one block of contiguous type. 
      char c;
      iarc >> c;
      column_type = (flex_type_enum)c;
      // all undefined. generate and return
      if (column_type == flex_type_enum::UNDEFINED) {
        for (size_t i = 0;i < dsize; ++i) callback(FLEX_UNDEFINED);
        return true;
      }
    } else if (num_types == 2) {
      // two types, but with undefined entries.
      char c;
      iarc >> c;
      column_type = (flex_type_enum)c;
      // read the bitset and undefine all the flagged entries
      undefined_bitmap.resize(info.num_elem);
      undefined_bitmap.clear();
      iarc.read((char*)undefined_bitmap.array, sizeof(size_t)*undefined_bitmap.arrlen);
      num_undefined = undefined_bitmap.popcount();
    }
  } else {
    std::vector<flexible_type> values;
    iarc >> values;
    for (const auto& i: values) {
      callback(i);
    }
  }

  if (perform_type_decoding) {
    int last_id = 0;
    auto stream_callback = 
        [&](const flexible_type& val) {
          // generate all the undefined
          if (num_undefined) {
            while(last_id < dsize && 
                  undefined_bitmap.get(last_id)) {
              callback(FLEX_UNDEFINED);
              ++last_id;
            }
          }
          callback(val);
          ++last_id;
        };
    size_t elements_to_decode = dsize - num_undefined;
    if (column_type == flex_type_enum::INTEGER) {
      decode_number_stream(elements_to_decode, iarc, stream_callback); 
    } else if (column_type == flex_type_enum::FLOAT) {
      if (info.flags & BLOCK_ENCODING_EXTENSION) {
        decode_double_stream(elements_to_decode, iarc, stream_callback); 
      } else {
        decode_double_stream_legacy(elements_to_decode, iarc, stream_callback); 
      }
    } else if (column_type == flex_type_enum::STRING) {
      decode_string_stream(elements_to_decode, iarc, stream_callback); 
    } else if (column_type == flex_type_enum::VECTOR) {
      decode_vector_stream(elements_to_decode, iarc, stream_callback, 
                           info.flags & BLOCK_ENCODING_EXTENSION); 
    } else {
      flexible_type_impl::deserializer s{iarc};
      flexible_type ret(column_type);
      for (size_t i = 0;i < dsize; ++i) {
        if (num_undefined && undefined_bitmap.get(i)) {
          callback(FLEX_UNDEFINED);
        } else {
          ret.apply_mutating_visitor(s);
          callback(ret);
        }
      }
    }
    // generate the final undefined values
    if (num_undefined) {
      while(last_id < dsize && 
            undefined_bitmap.get(last_id)) {
        callback(FLEX_UNDEFINED);
        ++last_id;
      }
    }
  }
  return true;
}

} // namespace v2_block_impl
} // namespace graphlab
#endif
