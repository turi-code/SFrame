/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <logger/logger.hpp>
#include <fileio/general_fstream.hpp>
#include <fileio/fs_utils.hpp>
#include <boost/tokenizer.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/concepts.hpp>
#include <boost/iostreams/operations.hpp> 
#include <boost/iostreams/device/file.hpp>
#include <sframe/sframe.hpp>
#include <sframe/csv_line_tokenizer.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <sframe/comma_escape_string.hpp>
#include <flexible_type/flexible_type_spirit_parser.hpp>

using namespace graphlab;

size_t infer_schema(const std::string & type_hints,std::vector<flex_type_enum> & column_types)  { 
  boost::char_separator<char> sep(",");
  boost::tokenizer< boost::char_separator<char> > tok(type_hints, sep);
  boost::tokenizer< boost::char_separator<char> >::iterator beg = tok.begin();
  size_t index = 0;
  while(beg != tok.end()) { 
    std::string _type = (*beg);
    if(_type == "int") { 
      column_types[index] = flex_type_enum::INTEGER;
    } 
    else if(_type == "float") { 
      column_types[index] = flex_type_enum::FLOAT;
    } 
    else if(_type == "str" || _type == "unicode") { 
      column_types[index] = flex_type_enum::STRING;
    } 
    else { 
      log_and_throw("Only basic types int,float,str are supported at this time."); 
    }  
    index++;  
    beg++;
  }
  return index;
}

void initialize_tokens(const std::vector<flex_type_enum> & column_types,std::vector<flexible_type> & tokens) { 
  for(size_t i=0;i<column_types.size();i++) { 
    if(column_types[i] != flex_type_enum::STRING) { 
      tokens[i].reset(column_types[i]); 
    }
  }
}

int main(int argc, char **argv) {
  /**
   * rddtosframe-nonpickle.cpp reads rdd rows from the stdin, does type inference, and save 
   * the result in the output sframe. In python side, rddtosf_nonpickle binary is passed to rdd.pipe() operator when 
   * rdd is not pickled which means it is UTF deserialized (__from_UTF8Deserialized_rdd__() )
   * This binary accepts two arguments. The first specifies the output directory for the output sframe. 
   * The second is type-hints in case of schema-rdd. This code first tries to find the first non-empty line 
   * and uses that line to infer the schema for the output sframe, then it iterates over all the remaining lines 
   * and adds them to the output sframe 
   */
  if(argc < 2) {
    std::cerr << "Usage: " << argv[0] << " <output directory> <type-hints> " <<  std::endl;
    return -1;
  }
  std::string output_directory(fileio::convert_to_generic(std::string(argv[1])));
  std::string type_hints = "";
  if(argc == 3)
    type_hints = std::string(argv[2]);

  /// Set up tokenizer options
  csv_line_tokenizer tokenizer;
  /// stdin receives each record in schema-rdd as a comma-separated string. 
  /// only use the comma-separated tokenizer if it is a schemaRDD 
  if(argc == 3) { /// it has type-hints argument, so it is a schemaRDD.
    tokenizer.delimiter = ',';
  } else {
    tokenizer.delimiter = '\n';
  }

  tokenizer.comment_char = '\0';
  tokenizer.escape_char = '\\';
  tokenizer.double_quote = true;
  tokenizer.quote_char = '\"';
  tokenizer.skip_initial_space = true;
  tokenizer.na_values.clear();
  tokenizer.init();

  std::vector<std::string> column_names;
  std::string first_line; /// the first line from the rdd with some content
  int num_skip_lines = -1; /// how many lines we skip before reaching a line with content
  std::vector<std::string> first_line_tokens;

  std::istream& fin = std::cin;
  if (!fin.good()) {
    log_and_throw("Fail reading from standard input");
  }

  /// read until a line with content is hit. 
  while(first_line.empty() || first_line.length() == 0) {   
    num_skip_lines++;
    if(!std::getline(fin,first_line)) { 
      return 0;
    }
    //boost::algorithm::trim(first_line); /// trimming should not be needed?? 
    tokenizer.tokenize_line(first_line.c_str(), 
        first_line.length(), 
        first_line_tokens);
  }

  size_t ncols = first_line_tokens.size();
  if (ncols == 0) log_and_throw(std::string("No data received from input pipe!"));

  column_names.resize(ncols);
  for (size_t i = 0;i < ncols; ++i) {
    column_names[i] = "X" + std::to_string(i + 1);
  }
  std::vector<flex_type_enum> column_types = std::vector<flex_type_enum>(ncols, flex_type_enum::STRING);
  std::vector<flexible_type> tokens = std::vector<flexible_type>(ncols,flex_string());
  /// only used for inserting empty records to the output sframe.  
  std::vector<flexible_type> empty_tokens;

  std::shared_ptr<flexible_type_parser> parser(new flexible_type_parser(tokenizer.delimiter,tokenizer.escape_char)); 
  /// if the number of argument is 3 then it means schema-rdd.pipe(). 
  if (argc == 3) { 
    /// it is a schema-rdd. We do basic type-inference for schema-rdd from the type_hints
    size_t index = infer_schema(type_hints,column_types); 
    if(index != ncols) { 
      log_and_throw("number of type_hints is not equal to number of actual columns"); 
    } 
    initialize_tokens(column_types,tokens);

  } else {
    /// NOT an schema-rdd
    /// first_line is already trimmed
    const char* first_line_ptr = first_line.c_str();
    std::pair<flexible_type, bool> ret = 
      parser->general_flexible_type_parse(&(first_line_ptr),first_line.size());
    if(ret.second && first_line_ptr == first_line.c_str() + first_line.size()) { 
      /// We know that ncols is one
      /// here we also know that the ret.first.get_type() is flex_type_enum::STRING
      column_types[0] = ret.first.get_type();
      tokens[0].reset(ret.first.get_type());
    } 
  }
  /// initializing empty_tokens
  empty_tokens = tokens;

  // Create the SFrame to write to
  //TODO: Repeat this until done
  boost::uuids::uuid file_prefix = boost::uuids::random_generator()();
  std::stringstream ss;
  ss << output_directory << "/" << file_prefix << ".frame_idx";
  sframe frame;
  frame.open_for_write(column_names, column_types, "", 1, false);

  std::string output="";
  size_t output_len = 0; 
  auto it_out = frame.get_output_iterator(0);

  /// First write the skip lines into ouput 
  for(size_t n=0;n<num_skip_lines;n++) {
    *it_out = empty_tokens;
    ++it_out;
  }
  /// re-read the first line that was used for probing
  std::string curVal = first_line;
  do {    
    size_t _num_col = tokenizer.tokenize_line(&(curVal[0]),curVal.size(),
        tokens,true);

    // schema-rdd records fetched from stdin as a comma seperated strings 
    // The ncols is the number of comma-separated items in a non-empty record.
    // Here if we receive an empty string, _num_col would not be equal to ncols. 
    // In this case we ouput an empty_token. 
    if(_num_col != ncols){
      *it_out = empty_tokens;
      ++it_out;
      continue;
    }

    /// tokens with flex_type_enum::STRING types need unescaping 
    for(size_t i=0;i<tokens.size();i++) { 
      if(tokens[i].get_type() == flex_type_enum::STRING) { 
        comma_unescape_string(tokens[i].get<flex_string>(),output,output_len);
        output.resize(output_len);
        tokens[i].mutable_get<flex_string>() = output;
      }
    }
    *it_out = tokens;
    ++it_out;
  } while(std::getline(fin, curVal)); 

  if(frame.is_opened_for_write()) {
    frame.close();
  }
  std::string index_str = ss.str();
  frame.save(index_str);

  // Only send index path if save succeeded
  std::cout << index_str << std::endl;

  //fin.close();
  return 0;
}
