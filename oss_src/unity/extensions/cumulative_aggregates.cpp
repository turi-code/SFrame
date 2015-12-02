/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <parallel/pthread_tools.hpp>
#include <parallel/lambda_omp.hpp>
#include <unity/lib/toolkit_function_macros.hpp>
#include <unity/extensions/cumulative_aggregates.hpp>

namespace graphlab {
namespace cumulative_aggregates {


/**
 * Throw an error if a vector is of unequal length.
 * \param[in] gl_sarray of type vector 
 */
void check_vector_equal_size(const gl_sarray& in) {
  
  // Initialize. 
  DASSERT_TRUE(in.dtype() == flex_type_enum::VECTOR); 
  size_t n_threads = thread::cpu_count();
  DASSERT_GE(n_threads, 1);
  size_t m_size = in.size();
          
  // Throw the following error. 
  auto throw_error = [] (size_t row_number, size_t expected, size_t current) {
    std::stringstream ss;
    ss << "Vectors must be of the same size. Row " << row_number 
       << " contains a vector of size " << current << ". Expected a vector of"
       << " size " << expected << "." << std::endl;
    log_and_throw(ss.str());
  };
  

  // Within each block of the SArray, check that the vectors have the same size.
  std::vector<size_t> expected_sizes (n_threads, size_t(-1));
  in_parallel([&](size_t thread_idx, size_t n_threads) {
    size_t start_row = thread_idx * m_size / n_threads; 
    size_t end_row = (thread_idx + 1) * m_size / n_threads;
    size_t expected_size = size_t(-1);
    size_t row_number = start_row;
    for (const auto& v: in.range_iterator(start_row, end_row)) {
      if (v != FLEX_UNDEFINED) {
        if (expected_size == size_t(-1)) {
          expected_size = v.size();
          expected_sizes[thread_idx] = expected_size; 
        } else {
          DASSERT_TRUE(v.get_type() == flex_type_enum::VECTOR);
          if (expected_size != v.size()) {
            throw_error(row_number, expected_size, v.size());
          }
        }
      }
      row_number++;
    }
  });

  // Make sure sizes accross blocks are also the same. 
  size_t vector_size = size_t(-1);
  for (size_t thread_idx = 0; thread_idx < n_threads; thread_idx++) {
    // If this block contains all None values, skip it.
    if (expected_sizes[thread_idx] != size_t(-1)) {

      if (vector_size == size_t(-1)) {
          vector_size = expected_sizes[thread_idx]; 
      } else {
         if (expected_sizes[thread_idx] != vector_size) {
           throw_error(thread_idx * m_size / n_threads, 
                              vector_size, expected_sizes[thread_idx]);
         } 
      }
    }
  }

}

gl_sarray _sarray_cumulative_built_in_aggregate(const gl_sarray& in, 
                                                const std::string& name) {
  flex_type_enum input_type = in.dtype();
  std::shared_ptr<group_aggregate_value> aggregator;

  // Cumulative sum, and avg support vector types.
  if (name == "__builtin__cum_sum__") {
    switch(input_type) {
      case flex_type_enum::VECTOR: {
        check_vector_equal_size(in);
        aggregator = get_builtin_group_aggregator(std::string("__builtin__vector__sum__")); 
        break;
      }
      default:
        aggregator = get_builtin_group_aggregator(std::string("__builtin__sum__")); 
        break;
    }
  } else if (name == "__builtin__cum_avg__") {
    switch(input_type) {
      case flex_type_enum::VECTOR: {
        check_vector_equal_size(in);
        aggregator = get_builtin_group_aggregator(std::string("__builtin__vector__avg__")); 
        break;
      }
      default:
        aggregator = get_builtin_group_aggregator(std::string("__builtin__avg__")); 
        break;
    }
  } else if (name == "__builtin__cum_max__") {
      aggregator = get_builtin_group_aggregator(std::string("__builtin__max__")); 
  } else if (name == "__builtin__cum_min__") {
      aggregator = get_builtin_group_aggregator(std::string("__builtin__min__")); 
  } else if (name == "__builtin__cum_var__") {
      aggregator = get_builtin_group_aggregator(std::string("__builtin__var__")); 
  } else if (name == "__builtin__cum_std__") {
      aggregator = get_builtin_group_aggregator(std::string("__builtin__stdv__")); 
  } else {
    log_and_throw("Internal error. Unknown cumulative aggregator " + name);
  }
  return in.cumulative_aggregate(aggregator);
}


BEGIN_FUNCTION_REGISTRATION
REGISTER_FUNCTION(_sarray_cumulative_built_in_aggregate, "in", "name");
END_FUNCTION_REGISTRATION

}
}
