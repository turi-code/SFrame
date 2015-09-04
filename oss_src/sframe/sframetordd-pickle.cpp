/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <string>
#include <iostream> 
#include <random>
#include <string>
#include <set>
#include <sstream>
#include <string.h>
#include <stdio.h>
#include <vector> 
#include <sframe/sframe_iterators.hpp>
#include <iterator>
#include <sframe/csv_writer.hpp>
#include <boost/filesystem.hpp>
#include <boost/tokenizer.hpp>
#include <sframe/comma_escape_string.hpp>
#include <flexible_type/string_escape.hpp>
#include <boost/python.hpp>
#include <lambda/pyflexible_type.hpp>

using namespace graphlab;
namespace python = boost::python;
/**
 * sframetordd-pickle.cpp reads from stdin a range [row_start,row_end) and output
 * a new sframe that only contains rows from input sframe with their indices i inside
 * the range row_start <= i < row_end. 
 *
 **/

inline void utf8_encode(const std::string& val, 
                   std::string& output, size_t& output_len) {
  if (output.size() < 2 * val.size()) {
    output.resize(2 * val.size());
  }
  char byte1 = 0xC2;
  char* cur_out = &(output[0]);
  // loop through the input string
  for (size_t i = 0; i < val.size(); ++i) {
    char c = val[i];
    if((c & 0x80) == 0) { 
      (*cur_out++) = c; 
    } else { 
      byte1 = 0xC2;
      if(c & (1 << 6))  
        byte1 = 0xC3; 
      (*cur_out++) = byte1;       
      (*cur_out++) = (c & 0xBF);
    }  
 }
 output_len = cur_out - &(output[0]);
}

int main(int argc, char **argv) {

  if(argc != 2) {
    std::cerr << "Usage: " << argv[0] << " <sframe location>" << std::endl;
    return 1;
  }

  std::string location = argv[1];
  size_t row_start,row_end;
  
  std::string range;
  std::getline(std::cin,range);

  boost::char_separator<char> sep(":");
  /// the fix for spark1.2: removes '[' and ']'  
  range.erase(std::remove(range.begin(), range.end(), '['), range.end());
  range.erase(std::remove(range.begin(), range.end(), ']'), range.end());
  boost::tokenizer< boost::char_separator<char> > tok(range, sep);
  boost::tokenizer< boost::char_separator<char> >::iterator beg = tok.begin();
  
  ASSERT_MSG(beg != tok.end(),"Range format is wrong");
  row_start = std::stoi(*beg); 
  beg++;
  ASSERT_MSG(beg != tok.end(),"Range format is wrong");
  row_end = std::stoi(*beg);
 
  dir_archive dirarc;
  dirarc.open_directory_for_read(location);
  std::string content_value;
  if (dirarc.get_metadata("contents", content_value) == false ||
      content_value != "sframe") {
        log_and_throw_io_failure("Archive does not contain an SFrame");
  }
  std::string prefix = dirarc.get_next_read_prefix();
  auto sframe_ptr = std::make_shared<sframe>(prefix + ".frame_idx");
  
  const std::vector<std::string>& column_names = sframe_ptr->column_names();
  Py_Initialize(); 
  lambda::import_modules();
  python::object pickle = python::import("cPickle");
  python::object base64 = python::import("base64");
  
  csv_writer writer;
  parallel_sframe_iterator_initializer it_init(*sframe_ptr,row_start,row_end);
  size_t n_threads = graphlab::thread_pool::get_instance().size();
  for(size_t i = 0; i < n_threads; ++i) {
    for(parallel_sframe_iterator it(it_init, i, n_threads); !it.done(); ++it) {
      std::vector<flexible_type> row;
      it.fill(row);
      python::dict d;
      for (size_t i = 0;i < row.size(); ++i) {
        if (row[i].get_type() == flex_type_enum::DATETIME) { 
          d[lambda::PyObject_FromFlex(flex_string(column_names[i]))] = lambda::PyObject_FromFlex(flex_string(row[i]));
        }
        else { 
          d[lambda::PyObject_FromFlex(flex_string(column_names[i]))] = lambda::PyObject_FromFlex(row[i]);
        } 
      } 
      python::object pickle_dict = python::object(pickle.attr("dumps")(d,python::object(python::handle<>(PyLong_FromLong(2)))));
      
      python::object encoded_obj = python::object(base64.attr("b64encode")(pickle_dict));
      
      std::string stringPickle =  python::extract<std::string>(encoded_obj);
      /// good for debugging 
      /*std::cout << stringPickle.length() << "\n";
      for (char c: stringPickle) {
        std::cout << int(c) << "\t";
      }
      std::cout << "\n";*/
      std::cout << stringPickle << std::endl; 
    }
  }
  return 0;

}
