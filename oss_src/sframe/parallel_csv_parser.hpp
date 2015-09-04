/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_UNITY_LIB_PARALLEL_CSV_PARSER_HPP
#define GRAPHLAB_UNITY_LIB_PARALLEL_CSV_PARSER_HPP
#include <string>
#include <vector>
#include <map>
#include <flexible_type/flexible_type.hpp>
#include <sframe/sframe.hpp>
#include <sframe/csv_line_tokenizer.hpp>
#include <sframe/sframe_constants.hpp>
namespace graphlab {

std::istream& eol_safe_getline(std::istream& is, std::string& t);

/**
 * All the options pertaining to top level CSV file handling
 */
struct csv_file_handling_options {
  /// Whether the first (non-commented) line of the file is the column name header.
  bool use_header = true; 
  
  /// Whether we should just skip line errors.
  bool continue_on_failure = false; 

  /// Whether failed parses will be stored in an sarray of strings and returned.
  bool store_errors = false;

  /// collection of column name->type. Every other column type will be parsed as a string
  std::map<std::string, flex_type_enum> column_type_hints;

  /// Output column names
  std::vector<std::string> output_columns;

  /// The number of rows to read.  If 0, all lines are read
  size_t row_limit = 0; 

  /// Number of rows at the start of each file to ignore
  size_t skip_rows = 0;
};

std::map<std::string, std::shared_ptr<sarray<flexible_type>>> parse_csvs_to_sframe(
    const std::string& url,
    csv_line_tokenizer& tokenizer,
    csv_file_handling_options options,
    sframe& frame,
    std::string frame_sidx_file = "");

}

#endif // GRAPHLAB_UNITY_LIB_PARALLEL_CSV_PARSER_HPP
