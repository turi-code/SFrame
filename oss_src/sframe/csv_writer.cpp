/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <sframe/csv_writer.hpp>
#include <flexible_type/string_escape.hpp>
#include <logger/logger.hpp>
namespace graphlab {

void csv_writer::write_verbatim(std::ostream& out,
                                const std::vector<std::string>& row) {
  for (size_t i = 0;i < row.size(); ++i) {
    out << row[i];
    // put a delimiter after every element except for the last element.
    if (i + 1 < row.size()) out << delimiter;
  }
  out << line_terminator;
}

void csv_writer::csv_print_internal(std::string& out, const flexible_type& val) {
  switch(val.get_type()) {
    case flex_type_enum::INTEGER:
    case flex_type_enum::FLOAT:
      out += std::string(val);
      break;
    case flex_type_enum::DATETIME:
    case flex_type_enum::VECTOR:
      out += std::string(val);
      break;
    case flex_type_enum::STRING:
      escape_string(val.get<flex_string>(), escape_char, 
                    quote_char, true, false, 
                    m_string_escape_buffer, m_string_escape_buffer_len);
      out += std::string(m_string_escape_buffer.c_str(), m_string_escape_buffer_len);
      break;
    case flex_type_enum::LIST:
      out += '[';
      for(size_t i = 0;i < val.get<flex_list>().size(); ++i) {
        csv_print_internal(out, val.get<flex_list>()[i]);
        if (i + 1 < val.get<flex_list>().size()) out += ',';
      }
      out += ']';
      break;
    case flex_type_enum::DICT:
      out += '{';
      for(size_t i = 0;i < val.get<flex_dict>().size(); ++i) {
        csv_print_internal(out, val.get<flex_dict>()[i].first);
        out += ':';
        csv_print_internal(out, val.get<flex_dict>()[i].second);
        if (i + 1 < val.get<flex_dict>().size()) out += ',';
      }
      out += '}';
      break;
    case flex_type_enum::UNDEFINED:
      break;
    default:
      out += (std::string)val;
      break;
  }
}

void csv_writer::csv_print(std::ostream& out, const flexible_type& val) {
  switch(val.get_type()) {
    case flex_type_enum::INTEGER:
    case flex_type_enum::FLOAT:
      if (quote_level == csv_quote_level::QUOTE_ALL) {
        out << quote_char << std::string(val) << quote_char; // quote numbers only at QUOTE_ALL
      } else {
        out << std::string(val);
      }
      break;
    case flex_type_enum::DATETIME:
    case flex_type_enum::VECTOR:
      if (quote_level != csv_quote_level::QUOTE_NONE) {
        // quote this field at any level higher than QUOTE_NONE
        out << quote_char << std::string(val) << quote_char;
      } else {
        out << std::string(val);
      }
      break;
    case flex_type_enum::STRING:
      if (quote_level != csv_quote_level::QUOTE_NONE) {
        escape_string(val.get<flex_string>(), escape_char, 
                      quote_char, 
                      true,
                      double_quote,
                      m_string_escape_buffer, m_string_escape_buffer_len);
        out.write(m_string_escape_buffer.c_str(), m_string_escape_buffer_len);
      } else {
        escape_string(val.get<flex_string>(), escape_char, 
                      quote_char, 
                      false,
                      false,
                      m_string_escape_buffer, m_string_escape_buffer_len);
        out.write(m_string_escape_buffer.c_str(), m_string_escape_buffer_len);
      }
      break;
    case flex_type_enum::LIST:
    case flex_type_enum::DICT:
      if (quote_level != csv_quote_level::QUOTE_NONE) {
        m_complex_type_temporary.clear();
        csv_print_internal(m_complex_type_temporary, val);
        escape_string(m_complex_type_temporary, escape_char, 
                      quote_char, 
                      true,
                      double_quote,
                      m_complex_type_escape_buffer, 
                      m_complex_type_escape_buffer_len);
        out.write(m_complex_type_escape_buffer.c_str(), m_complex_type_escape_buffer_len);
      } else {
        m_complex_type_temporary.clear();
        csv_print_internal(m_complex_type_temporary, val);
        out.write(m_complex_type_temporary.c_str(), m_complex_type_temporary.length());
      }
      break;
    case flex_type_enum::UNDEFINED:
      if (quote_level != csv_quote_level::QUOTE_ALL) {
        out.write(na_value.c_str(), na_value.length());
      } else {
        out << quote_char << na_value << quote_char;
      }
      break;
    default:
      if (quote_level != csv_quote_level::QUOTE_NONE) {
        // quote this field at any level higher than QUOTE_NONE
        out << quote_char << std::string(val) << quote_char;
      } else {
        out << std::string(val);
      }
      break;
  }
}

void csv_writer::write(std::ostream& out, 
                       const std::vector<flexible_type>& row) {
  for (size_t i = 0;i < row.size(); ++i) {
    csv_print(out, row[i]);
    // put a delimiter after every element except for the last element.
    if (i + 1 < row.size()) out << delimiter;
  }
  out << line_terminator;
}



} // namespace graphlab
