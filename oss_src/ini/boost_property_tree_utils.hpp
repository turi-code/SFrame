/*
 * Copyright (C) 2015 Dato, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#ifndef GRAPHLAB_INI_BOOST_PROPERTY_TREE_UTILS_HPP
#define GRAPHLAB_INI_BOOST_PROPERTY_TREE_UTILS_HPP

#define BOOST_SPIRIT_THREADSAFE

#include <map>
#include <vector>
#include <string>
#include <boost/property_tree/ptree.hpp>
#include <boost/lexical_cast.hpp>
#include <logger/logger.hpp>
namespace graphlab {
namespace ini {

/**
 * Reads a key in an ini/json file as a sequence of values. In the ini file
 * this will be represented as 
 *
 * [key]
 * 0000 = "hello"
 * 0001 = "pika"
 * 0002 = "chu"
 *
 * But in a JSON file this could be
 * {"0000":"hello","0001":"pika","0002":"chu"}
 * or 
 * ["hello","pika","chu"]
 * depending on which writer is used. (The boost property tree writer will 
 * create the first, a regular JSON writer will create the second).
 * 
 * This will return a 3 element vector containing {"hello", "pika", "chu"}
 *
 * \see write_sequence_section
 */
template <typename T>
std::vector<T> read_sequence_section(const boost::property_tree::ptree& data, 
                                     std::string key,
                                     size_t expected_elements) {
  std::vector<T> ret;
  if (expected_elements == 0) return ret;
  const boost::property_tree::ptree& section = data.get_child(key);
  ret.resize(expected_elements);

  // loop through the children of the column_names section
  size_t sid = 0;
  for(const auto& val: section) {
    const auto& key = val.first;
    if (key.empty()) {
      // this is the array-like sequences
      ret[sid] = boost::lexical_cast<T>(val.second.get_value<std::string>());
      ++sid;
    } else {
      // this is a dictionary-like sequence
      sid = std::stoi(key);
      if (sid >= ret.size()) {
        log_and_throw(std::string("Invalid ID in ") + key + " section."
                      "Segment IDs are expected to be sequential.");
      }
      ret[sid] = boost::lexical_cast<T>(val.second.get_value<std::string>());
    }
  }
  return ret;
}




/**
 * Reads a key in an ini/json file as a dictionary of values. In the ini file
 * this will be represented as 
 *
 * [key]
 * fish = "hello"
 * and = "pika"
 * chips = "chu"
 *
 * In a JSON file this will be represented as 
 * {"fish":"hello", "and":"pika", "chips":"chu"}
 *
 * This will return a 3 element map containing 
 * {"fish":"hello", "and":"pika", "chips":"chu"}.
 *
 * \see write_dictionary_section
 */
template <typename T>
std::map<std::string, T> read_dictionary_section(const boost::property_tree::ptree& data, 
                                                 std::string key) {
  std::map<std::string, T> ret;
  // no section found
  if (data.count(key) == 0) {
    return ret;
  }
  const boost::property_tree::ptree& section = data.get_child(key);

  // loop through the children of the column_names section
  for(const auto& val: section) {
      ret.insert(std::make_pair(val.first,
                                val.second.get_value<T>()));
  }
  return ret;
}


/**
 * Writes a vector of values into an ini file as a section. 
 *
 * For instance, given a 3 element vector containing {"hello", "pika", "chu"}
 * The vector be represented as 
 *
 * [key]
 * 0000 = "hello"
 * 0001 = "pika"
 * 0002 = "chu"
 *
 * \see read_sequence_section
 * 
 */
template <typename T>
void write_sequence_section(boost::property_tree::ptree& data, 
                            const std::string& key,
                            const std::vector<T>& values) {
  for (size_t i = 0; i < values.size(); ++i) {
    // make the 4 digit suffix
    std::stringstream strm;
    strm.fill('0'); strm.width(4); strm << i;
    data.put(key + "." + strm.str(), values[i]);
  }
}

/**
 * Writes a dictionary of values into an ini file as a section. 
 * For instance, given a 3 element map containing 
 * {"fish":"hello", "and":"pika", "chips":"chu"}.
 *
 * In the ini file this will be represented as:
 *
 * [key]
 * fish = "hello"
 * and = "pika"
 * chips = "chu"
 *
 * \see read_dictionary_section
 *
 */
template <typename T>
void write_dictionary_section(boost::property_tree::ptree& data, 
                            const std::string& key,
                            const std::map<std::string, T>& values) {
  // Write out metadata
  std::string m_heading = key + ".";
  for(const auto& map_entry : values) {
    std::string mkey(m_heading);
    mkey += map_entry.first;
    data.put(mkey, map_entry.second);
  }
}


} // ini
} // namespace graphlab
#endif
