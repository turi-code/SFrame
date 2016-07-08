/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#ifndef GRAPHLAB_FLEXIBLE_TYPE_SPIRIT_PARSER_HPP
#define GRAPHLAB_FLEXIBLE_TYPE_SPIRIT_PARSER_HPP

#include <boost/spirit/include/support_utree.hpp>
#include <flexible_type/flexible_type.hpp>
#include <flexible_type/string_parser.hpp>
#include <boost/spirit/include/qi_char_class.hpp>

namespace graphlab {

using namespace boost::spirit;

/**
 * The actual grammar definitions
 */
template <typename Iterator, typename SpaceType>
struct flexible_type_parser_impl;




/**
 * A flexible_type_parser which takes in strings and returns flexible_types
 */
class flexible_type_parser {
 public:
  flexible_type_parser(std::string delimiter = ",", char escape_char = '\\');
  /**
   * Parses a generalized flexible type from a string. The *str pointer will be
   * updated to point to the character after the last character parsed.
   * Returns a pair of (parsed value, success)
   */
  std::pair<flexible_type, bool>
      general_flexible_type_parse(const char** str, size_t len);

  /**
   * Parses a non-string flexible type from a string. The *str pointer will be
   * updated to point to the character after the last character parsed.
   * Returns a pair of (parsed value, success)
   */
  std::pair<flexible_type, bool>
      non_string_flexible_type_parse(const char** str, size_t len);

  /**
   * Parses a flex_dict from a string. The *str pointer will be
   * updated to point to the character after the last character parsed.
   * Returns a pair of (parsed value, success)
   */
  std::pair<flexible_type, bool>
      dict_parse(const char** str, size_t len);

  /**
   * Parses a flex_list from a string. The *str pointer will be
   * updated to point to the character after the last character parsed.
   * Returns a pair of (parsed value, success)
   */
  std::pair<flexible_type, bool>
      recursive_parse(const char** str, size_t len);

  /**
   * Parses a flex_vec from a string. The *str pointer will be
   * updated to point to the character after the last character parsed.
   * Returns a pair of (parsed value, success)
   */
  std::pair<flexible_type, bool>
      vector_parse(const char** str, size_t len);

  /**
   * Parses a double from a string. The *str pointer will be
   * updated to point to the character after the last character parsed.
   * Returns a pair of (parsed value, success)
   */
  std::pair<flexible_type, bool>
      double_parse(const char** str, size_t len);

  /**
   * Parses an integer from a string. The *str pointer will be
   * updated to point to the character after the last character parsed.
   * Returns a pair of (parsed value, success)
   */
  std::pair<flexible_type, bool>
      int_parse(const char** str, size_t len);


  /**
   * Parses an string from a string. The *str pointer will be
   * updated to point to the character after the last character parsed.
   * Returns a pair of (parsed value, success)
   */
  std::pair<flexible_type, bool>
      string_parse(const char** str, size_t len);

 private:
  std::shared_ptr<flexible_type_parser_impl<const char*, decltype(boost::spirit::iso8859_1::space)> > parser;
  std::shared_ptr<flexible_type_parser_impl<const char*, decltype(qi::eoi)> > non_space_parser;
  bool delimiter_has_space(const std::string& separator);
  bool m_delimiter_has_space;
};

} // namespace graphlab

#endif // GRAPHLAB_FLEXIBLE_TYPE_SPIRIT_PARSER_HPP
