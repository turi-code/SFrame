/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <logger/assertions.hpp>
#include <sframe/sframe_rows.hpp>

namespace graphlab {

void sframe_rows::resize(size_t num_cols, ssize_t num_rows) {
  ensure_unique();
  if (m_decoded_columns.size() != num_cols) m_decoded_columns.resize(num_cols);
  for (auto& col: m_decoded_columns) {
    if (col == nullptr) {
      if (num_rows == -1) {
        col = std::make_shared<decoded_column_type>();
      } else {
        col = std::make_shared<decoded_column_type>(num_rows, flex_undefined());
      }
    } else if (num_rows != -1 && col->size() != (size_t)num_rows) {
      col->resize(num_rows, flex_undefined());
    }
  }
}

void sframe_rows::clear() {
  m_decoded_columns.clear();
}

void sframe_rows::save(oarchive& oarc) const {
  oarc << m_decoded_columns.size();
  for (auto& i : m_decoded_columns) oarc << *i;
}

void sframe_rows::load(iarchive& iarc) {
  size_t ncols = 0;
  iarc >> ncols;
  resize(ncols);
  for (size_t i = 0; i < ncols; ++i) {
    iarc >> *(m_decoded_columns[i]);
  }
}

void sframe_rows::add_decoded_column(
    const sframe_rows::ptr_to_decoded_column_type& decoded_column) {
  m_decoded_columns.push_back(decoded_column);
}

void sframe_rows::ensure_unique() {
  if (m_is_unique) return;
  for (auto& col: m_decoded_columns) {
    if (!col.unique()) {
      col = std::make_shared<decoded_column_type>(*col);
    }
  }
  m_is_unique = true;
}

void sframe_rows::type_check_inplace(const std::vector<flex_type_enum>& typelist) {
  ASSERT_EQ(typelist.size(), num_columns());
  // one pass for column type check
  for (size_t c = 0; c < num_columns(); ++c) {
    if (typelist[c] != flex_type_enum::UNDEFINED) {
      // assume no modification required first
      auto& arr = m_decoded_columns[c];
      auto length = arr->size();
      bool current_array_is_unique = arr.unique();
      size_t i = 0;
      // ok what we are going to do is to assume no modifications are required
      // even if we do not have out own copy of the array.
      // then if modifications are required, we break out and resume below.
      if (!current_array_is_unique) {
        for (;i < length; ++i) {
          const auto& val = (*arr)[i];
          if (val.get_type() != typelist[c] && 
              val.get_type() != flex_type_enum::UNDEFINED) {
            // damn. modifications are required
            arr = std::make_shared<decoded_column_type>(*arr);
            current_array_is_unique = true;
            break;
          }
        }
      }
      if (current_array_is_unique) {
        for (;i < length; ++i) {
          auto& val = (*arr)[i];
          if (val.get_type() != typelist[c] && 
              val.get_type() != flex_type_enum::UNDEFINED) {
            flexible_type res(typelist[c]);
            res.soft_assign(val);
            val = std::move(res);
          }
        }
      }
    }
  }
}

sframe_rows sframe_rows::type_check(const std::vector<flex_type_enum>& typelist) const {
  ASSERT_EQ(typelist.size(), num_columns());
  sframe_rows other = *this;
  other.type_check_inplace(typelist);
  return other;
}

} // namespace graphlab
