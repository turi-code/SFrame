/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#ifndef GRAPHLAB_FLEXIBLE_TYPE_STRING_ESCAPE_HPP
#define GRAPHLAB_FLEXIBLE_TYPE_STRING_ESCAPE_HPP
#include <string>
namespace graphlab {

/**
 * Unescapes a string inplace
 */
void unescape_string(std::string& cal, char escape_char, 
                     char quote_char, bool double_quote);

/**
 * Unescapes a string inplace, returning the new length
 */
size_t unescape_string(char* cal, 
                       size_t length, char escape_char, 
                       char quote_char, bool double_quote);
/**
 * Escapes a string from val into output.
 * The length of the output string is in returned in output_len.
 * Note that output.length() may be greater than the output_len.
 *
 * \param val The string to escape
 * \param escape_char The escape character to use (recommended '\\')
 * \param quote_char The quote character to use. (recommended '\"')
 * \param use_quote_char If the output string should be quoted
 * \param double_quote If double quotes are converted to single quotes.
 */
void escape_string(const std::string& val, 
                   char escape_char,
                   char quote_char,
                   bool use_quote_char,
                   bool double_quote,
                   std::string& output, 
                   size_t& output_len);


/**
 * Only perform double quoting escaping
 */
void double_quote_escape(const std::string& val,
                         std::string& output,
                         size_t& output_len);
} // namespace graphlab

#endif
