/*
* Copyright (C) 2015 Dato, Inc.
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Affero General Public License as
* published by the Free Software Foundation, either version 3 of the
* License, or (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU Affero General Public License for more details.
*
* You should have received a copy of the GNU Affero General Public License
* along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/
#include <Python.h>
#include <logger/logger.hpp>
#include <fileio/general_fstream.hpp>
#include <fileio/fs_utils.hpp>
#include <flexible_type/flexible_type.hpp>
#include <flexible_type/flexible_type_spirit_parser.hpp>
#include <sframe/sframe.hpp>
#include <sframe/csv_line_tokenizer.hpp>
#include <sframe/sframe_constants.hpp>
#include <sframe/sframe_saving.hpp> 
#include <sframe/sframe_iterators.hpp>
#include <sframe/csv_writer.hpp>
#include <sframe/comma_escape_string.hpp>
#include <lambda/pyflexible_type.hpp>
#include <boost/iostreams/device/file.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/python.hpp>
#include <boost/filesystem.hpp>
#include <boost/tokenizer.hpp>
#include <boost/program_options.hpp>


#include <string>
#include <iostream> 
#include <random>
#include <stdlib.h>
#include <sstream>
#include <stdio.h>
#include <vector> 
#include <iterator>
#include <climits>

using namespace graphlab;
namespace python = boost::python;
namespace bpo = boost::program_options;


/**
 * Read a binary encoded integer from standard in.
 */
int read_int() {
  int msgLen = 0;
  std::cin.read(reinterpret_cast<char*>(&msgLen), 4);
  /// @todo: test cost of verifying cin is good.
  // if (!std::cin.good()) {
  //   log_and_throw("Fail reading from standard input");
  // }
  return msgLen;
}

/**
 * Read a signed integer from the pointer to a location in a character
 * buffer
 */
int read_int(const char * & iter) {
  const int result = *(reinterpret_cast<const int*>(iter));
  iter += sizeof(int);
  return result;
}


/**
 * Read a message into buffer and return true if successful or 
 * false if end of stream.
 *
 * Note the buffer is automatically resized to store the exact length
 * of the message
 */
bool read_msg(std::vector<char>& buffer) {
  const int msgLen = read_int();
  //std::cerr << "Reading message of length: " << msgLen << std::endl;
  if (msgLen >= 0) {
    // Resize the buffer and fill it with data  
    buffer.resize(msgLen);
    std::cin.read(&buffer[0], msgLen);
    if (!std::cin.good()) {
      log_and_throw("Fail reading from standard input");
    }
    return true;
  }
  return false;
}


// /**
//  * Read a string message from binary standard in
//  */
// std::string read_string_msg() {
//   std::vector<char> buffer;
//   read_msg(buffer);
//   std::string text(&buffer[0], buffer.size());
//   return text;
// }


/**
 * Read a string message from binary standard in
 */
std::string read_string_msg(const char * & iter) {
  const int strlen = read_int(iter);
  // std::cerr << "reading string of length: " << strlen << std::endl;
  std::string text(iter, strlen);
  // std::cerr << "string: " << text << std::endl;
  iter += strlen;
  return text;
}


/**
 * Read a batch of flexible objects.
 */
flexible_type read_flex_obj(const std::vector<char> & buffer, 
                            const python::object & pickle) {
  // Make a python string corresponding to the first object
  python::object python_string = 
    python::object(python::handle<>(PyString_FromStringAndSize(&buffer[0],buffer.size())));
  // Unpickle the first object
  python::object unpickle_obj = python::object(pickle.attr("loads")(python_string));
  // get the flexible type corresponding to the python object
  return lambda::PyObject_AsFlex(unpickle_obj);
}


/**
 * Write a message to standard out in binary format encoded in the format:
 * message length (4 bytes) followed by bytes
 */
void write_msg(const char* buffer, size_t bufferLen) {
  // Write the message length as a signed int
  assert(bufferLen <= INT_MAX); // guard against integer wrap around for large arrays
  int msgLen(bufferLen);
  std::cout.write(reinterpret_cast<char*>(&msgLen), sizeof(int));
  // Write the message bytes
  std::cout.write(buffer, msgLen);
  if (!std::cout.good()) {
    log_and_throw("Fail writing to standard output");
  }
}


/**
 * Write a message to standard out in binary format encoded in the format:
 * message length (4 bytes) followed by bytes
 */
void write_msg(const std::vector<char> & buffer) {
  write_msg(&buffer[0], buffer.size());
}

/**
 * Parse the binary encoded schema exported by GraphLabUtil.scala.
 * These type signatures are defined in the Spark class:
 *  org.apache.spark.sql.types.DataTypes
 */
size_t parse_schema(const std::vector<char> & buffer,
  std::vector<std::string> & column_names, 
  std::vector<flex_type_enum> & column_types)  {
  const char* iter = &buffer[0];
  // Initialize the correct number of columns
  const int num_cols = read_int(iter);
  //std::cerr << "number of columns: " << num_cols << std::endl;
  column_types.resize(num_cols);
  column_names.resize(num_cols);
  for (size_t i = 0; i < num_cols; ++i) {
    // Read the column name
    column_names[i] = read_string_msg(iter);
    // Read the column type description string
    const std::string type_desc = read_string_msg(iter);
    //std::cerr << "Parsing schema for: " << column_names[i] 
    //          << ", type: " << type_desc << std::endl;
    // Based on the string define the column type
    if (type_desc == "byte" || type_desc == "short" || 
        type_desc == "int" || type_desc == "bigint") {
      column_types[i] = flex_type_enum::INTEGER;
    } else if (type_desc == "float" || type_desc == "double") {
      column_types[i] = flex_type_enum::FLOAT;
    } else if (type_desc == "string") {
      column_types[i] = flex_type_enum::STRING;
    } else if (type_desc == "boolean") { // encode booleans as integers
      column_types[i] = flex_type_enum::INTEGER;
    } else if (type_desc == "array<float>" || 
               type_desc == "array<double>" 
               // || 
               // type_desc == "array<byte>" || // Not supported yet
               // type_desc == "array<char>" ||
               // type_desc == "array<int>" || 
               // type_desc == "array<bigint>" ||
               //type_desc == "array<boolean>"
               ) {
      column_types[i] = flex_type_enum::VECTOR;
    } else if (type_desc.substr(0,5) == "array") {
      column_types[i] = flex_type_enum::LIST;
    } else if (type_desc == "date" || type_desc == "timestamp") {
      column_types[i] = flex_type_enum::DATETIME;
    } else { // if the type is none of the above it could be
      column_types[i] = flex_type_enum::DICT;
    } // @TODO: add datetime type handling
    // we need to figure out what spark uses for datetime name
    //std::cerr << "Type: " << flex_type_enum_to_name(column_types[i]) << std::endl;
  }
  return num_cols;
}


/**
 * Initialize tokens. Row_buffer is a container where column values with 
 * correct types will be saved.
 */
void initialize_row_buffer(const std::vector<flex_type_enum> & column_types, 
                           std::vector<flexible_type> & row_buffer) { 
  // Resize the tokens to match the number columns (populating with strings as a default)
  row_buffer.resize(column_types.size()); //, flex_string());       
  // Fill in the tokens with the correct type
  size_t ncols = column_types.size();
  for(size_t i = 0;i < ncols; ++i) {
    //    if(column_types[i] != flex_type_enum::STRING)
    row_buffer[i].reset(column_types[i]);
    // std::cerr << "Tokens: " << flex_type_enum_to_name(column_types[i]) << std::endl;
  }
}

std::string handle_pyerror() {
  using namespace boost::python;
  using namespace boost;

  PyObject *exc,*val,*tb;
  object formatted_list, formatted;
  PyErr_Fetch(&exc,&val,&tb);
  handle<> hexc(exc),hval(allow_null(val)),htb(allow_null(tb)); 
  object traceback(import("traceback"));
  if (!tb) {
    object format_exception_only(traceback.attr("format_exception_only"));
    formatted_list = format_exception_only(hexc,hval);
  } else {
    object format_exception(traceback.attr("format_exception"));
    formatted_list = format_exception(hexc,hval,htb);
  }
  formatted = boost::python::str("\n").join(formatted_list);
  return extract<std::string>(formatted);
}


/**
 * Infer the ouput sframe schema from the input row. 
 * The output is saved at column_names and column_types.
 */
void infer_schema(const flexible_type & row, 
                  bool is_dataframe,
                  std::vector<std::string> & column_names,
                  std::vector<flex_type_enum> & column_types) {
  size_t ncols = 1; 
  if(is_dataframe && row.get_type() == flex_type_enum::DICT) { 
    flex_dict dict = row.get<flex_dict>();
    ncols = dict.size();
    column_names.resize(ncols);
    column_types.resize(ncols);

    for (size_t i = 0;i < ncols; ++i) {
      column_names[i] = dict[i].first.get<flex_string>();
      column_types[i] = dict[i].second.get_type();
      if(column_types[i] == flex_type_enum::UNDEFINED) {
        column_types[i] = flex_type_enum::STRING;
      }
    }
  } else if(is_dataframe && row.get_type() == flex_type_enum::LIST) { 
    flex_list rec = row.get<flex_list>();
    ncols = rec.size();
    column_names.resize(ncols);
    column_types.resize(ncols);
    for(size_t i = 0;i < ncols; ++i) {
      column_names[i] = "X"+ std::to_string(i + 1);
      column_types[i] = rec[i].get_type();
      if(column_types[i] == flex_type_enum::UNDEFINED) {
        column_types[i] = flex_type_enum::STRING;
      }
    }
  } else if(is_dataframe && row.get_type() == flex_type_enum::VECTOR) { 
    flex_vec vec = row.get<flex_vec>();
    ncols = vec.size();
    column_names.resize(ncols);
    column_types.resize(ncols);
    for(size_t i = 0;i < ncols; ++i) {
      column_names[i] = "X"+ std::to_string(i + 1);
      column_types[i] = flex_type_enum::FLOAT;
    }
  } else { 
    column_names.resize(ncols); 
    column_types.resize(ncols);
    column_types[0] = row.get_type();
    if(column_types[0] == flex_type_enum::UNDEFINED) {
      column_types[0] = flex_type_enum::STRING;
    }
    column_names[0] = "X1";
  } 
}


/**
 * Populate the the tokens with the falues in the row.
 */
void populate_row_buffer(const flexible_type & row, 
                         bool is_dataframe,
                         std::vector<flexible_type> & row_buffer) {
  size_t ncols = 1;

  if (is_dataframe && row.get_type() == flex_type_enum::VECTOR) { 
    const flex_vec & vect = row.get<flex_vec>();
    ncols = vect.size();
    
    for (size_t i = 0;i < ncols; ++i) {
      row_buffer[i] = vect[i];
    }
  } else if (is_dataframe && row.get_type() == flex_type_enum::LIST) { 
    const flex_list & rec = row.get<flex_list>();
    ncols = rec.size();
    for (size_t i = 0; i < ncols; ++i) {
      if (row_buffer[i].get_type() == rec[i].get_type()) {
        // no casting necessary
        row_buffer[i] = rec[i];
      } else if (row_buffer[i].get_type() == flex_type_enum::VECTOR && 
        rec[i].get_type() == flex_type_enum::LIST) {
        // manual conversion necessary. This catches the case not handled by flexible_type
        // since the conversion below may fail.
        flex_vec & out = row_buffer[i].mutable_get<flex_vec>();
        const auto & in = rec[i].get<flex_list>();
        out.resize(in.size());
        for (size_t i = 0;i < in.size(); ++i) out[i] = in[i].to<double>();
      } else {
        // lets use the built in casting.
        row_buffer[i].soft_assign(rec[i]);
      }
    }
    // for (size_t i = 0; i < ncols; ++i) {
    //   if (column_types[i] == flex_type_enum::VECTOR) {
    //     row_buffer[i] = rec[i].get<flex_vec>();
    //   } else {
    //     row_buffer[i] = rec[i];
    //   }
    // }       
  } else if (is_dataframe && row.get_type() == flex_type_enum::DICT) {
    const flex_dict & dict = row.get<flex_dict>();
    ncols = dict.size();
    for (size_t i = 0;i < ncols; ++i) {
      row_buffer[i] = dict[i].second;
    }
  } else { 
    // this is not one of the standard code paths so just stuff into
    // the first column of the row buffer
    row_buffer[0] = row;
  }    
}


/**
 * Initialize all the schema variables based on the type information encoded in the buffer
 */
void initialize_schema_variables(const bool is_dataframe,
                                 const bool is_batch_mode,
                                 const std::vector<char> & buffer,
                                 const python::object & pickle,
                                 std::vector<std::string> & column_names,
                                 std::vector<flex_type_enum> & column_types,
                                 std::vector<flexible_type> & row_buffer,
                                 bool is_utf8_string) {
  if (is_dataframe) {
    // if its a datafame than the buffer contains the schema information
    parse_schema(buffer, column_names, column_types);
  } else { 
    // this is not a dataframe so we need to infer the schema by
    // looking at the types stored in the rows.
    flexible_type flex_obj;
    if (is_utf8_string) { 
      flex_obj = flex_string(&buffer[0],buffer.size());
    } else {
      flex_obj = read_flex_obj(buffer, pickle);
    }
    flexible_type first_row;
    // @todo: document this code (it makes no sense).
    if (is_batch_mode) {
      if (flex_obj.get_type() == flex_type_enum::LIST) {
        first_row = flex_obj.get<flex_list>()[0];
      } else { 
        // assert(flex_obj.get_type() == flex_type_enum::VECTOR)
        first_row = flex_obj[0];
      }
    } else {
      first_row = flex_obj;
    }
    // Infer the schema for the first row
    infer_schema(first_row, is_dataframe, column_names, column_types);   
  }
  // Initialize the row buffer based on the inferred type information
  initialize_row_buffer(column_types, row_buffer);
} 


/**
 * tosframe_main() reads rdd rows from the stdin, does type inference, and save 
 * the result in the output sframe. In python side, rddtosf_pickle binary is passed to
 * rdd.pipe() operator when rdd is pickled, either a batch of serialized objects or a 
 * single pickle object. This code uses the first line to infer the schema for the output
 * sframe, then it iterates over all the remaining lines and adds them to the output sframe.
 *
 * In all the following code scenarioes rddtosf_pickle binary is used:
 *
 * To read from rdd of lists to sframe, single pickle object:
 * >rdd = sc.parallelize([[2],[3,4],[]])
 * >sf = gl.SFrame.from_rdd(rdd)
 * >print sf
 *  +------------------------+
 *  |           X1           |
 *  +------------------------+
 *  |   array('d', [2.0])    |
 *  | array('d', [3.0, 4.0]) |
 *  |       array('d')       |
 *  +------------------------+
 *  [3 rows x 1 columns]
 * 
 * >rdd._jrdd_deserializer
 * <pyspark.serializers.PickleSerializer> 
 *
 * To read from rdd of dictionaries to sframe, batch of serialized objects:
 * >rdd = sc.parallelize([[2],[3,4],[]],1)
 * >sf = gl.SFrame.from_rdd(rdd)
 *
 * To read from schemardd of Rows to sframe: 
 * 
 * >from pyspark.sql import SQLContext, Row
 * >sqlContext = SQLContext(sc)
 * >lines = sc.parallelize([u'Emad, 79'])
 * >parts = lines.map(lambda l: l.split(","))
 * >people = parts.map(lambda p: Row(name=p[0], age=int(p[1])))
 * >schemaPeople = sqlContext.inferSchema(people)
 * >sf = gl.SFrame.from_rdd(schemaPeople)
 * >print sf
 * +-----+---------+
 * | age |   name  |
 * +-----+---------+
 * |  79 |   Emad  |
 * +-----+---------+
 */
int tosframe_main(const std::string & _output_directory, 
                  const std::string & _encoding, 
                  const std::string & _type) {
  try {
    bool is_batch_mode = false;
    bool is_dataframe = false;
    bool is_utf8_string = false;

    // Process the arguments
    if (_encoding == "batch") { // batch is the standard mode
      is_batch_mode = true;
    } else if (_encoding == "utf8" || _encoding == "pickle") {
      is_batch_mode = false;
    } else {
      std::cerr << "Unsupported encoding: " << _encoding << std::endl;
      return EXIT_FAILURE;
    }

    if(_encoding == "utf8") { 
      is_utf8_string = true;
    }
    
    if (_type == "dataframe" || _type == "DataFrame") { 
      is_dataframe = true;
    } else if (_type == "rdd" || _type == "RDD") {
      is_dataframe = false;
    } else  {
      std::cerr << "Unsupported rdd type: " << _type << std::endl;
      return EXIT_FAILURE;
    }    

    /// Initialze the boost.python and import necessary modules.  
    Py_NoSiteFlag=1;
    //logprogress_stream << "Py_Initialize called" << std::endl;
    Py_Initialize(); 
    lambda::import_modules();
    python::object pickle = python::import("cPickle");

    // Read the first line from the RDD
    std::vector<char> buffer;
    bool read_successful = read_msg(buffer);        
    if (!read_successful) {
      //std::cerr << "Empty RDD not supported!" << std::endl; 
      return 0;
    }

    // Compute the schema and initialize all the varialbes 
    std::vector<std::string> column_names;
    std::vector<flex_type_enum> column_types;
    std::vector<flexible_type> row_buffer;
    initialize_schema_variables(is_dataframe, is_batch_mode, buffer, pickle, 
                                column_names, column_types, row_buffer,is_utf8_string);

    // Open the sframe and initialize the output iterator
    sframe frame;
    sframe_output_iterator it_out;

    // Compute the final filename
    boost::uuids::uuid file_prefix = boost::uuids::random_generator()();
    std::stringstream ss;
    std::string output_directory = fileio::convert_to_generic(_output_directory);

    // @todo: make this a platform independent path.
    ss << output_directory << "/" << file_prefix << ".frame_idx";
    std::string sframe_idx_filename = fileio::convert_to_generic(ss.str());
    frame.open_for_write(column_names, column_types, sframe_idx_filename, 1, false);
    it_out = frame.get_output_iterator(0);

    // Dataframes contain an extra row of column information so read
    if (is_dataframe) {
      read_successful = read_msg(buffer);
    }
    
    // loop over messages writing to the sframe
    while (read_successful) {
      // Read the batch of rows into a flex type.
      flexible_type val;
      if (is_utf8_string) { 
        val = flex_string(&buffer[0],buffer.size());
      }
      else {
        val = read_flex_obj(buffer, pickle);
      }
      // @todo: document this code it makes no sense.      
      if (is_batch_mode && val.get_type() == flex_type_enum::VECTOR) {
        const flex_vec & vect_batch = val.get<flex_vec>();
        for(size_t index = 0 ; index < vect_batch.size() ; index++) { 
          row_buffer[0] = vect_batch[index];
          *it_out = row_buffer;
          ++it_out;
        }
      } else {
        // The standard case is batch mode and a list of rows
        if(is_batch_mode && val.get_type() == flex_type_enum::LIST) {
          const flex_list & batch = val.get<flex_list>();
          // Loop through the batch and copy to the sframe
          for(size_t index = 0 ; index < batch.size() ; index++) {  
            const flexible_type & row = batch[index];
            populate_row_buffer(row, is_dataframe, row_buffer);
            *it_out = row_buffer;
            ++it_out;
          }
        } else {
          // each record is a single row
          const flexible_type & row = val;
          populate_row_buffer(row, is_dataframe, row_buffer);
          *it_out = row_buffer;
          ++it_out;
        }
      }
      // Read the next buffer:
      read_successful = read_msg(buffer);
    } // end of while loop
    
    // Assert: we have read all the input from the RDD into the SFrame.
    // if we successfully opened the frame close
    if(frame.is_opened_for_write()) { // This should always be true?
      frame.close();
    }
    
    // print the final filename back to the calling process 
    std::cout << sframe_idx_filename << std::endl;
  
  } catch (python::error_already_set const& e) {
    if (PyErr_Occurred()) {
      std::string msg = handle_pyerror(); 
      std::cerr << "GRAPHLAB PYTHON-ERROR: " << msg << std::endl;
      //throw(msg);
    }
    //bool py_exception = true;
    python::handle_exception();
    PyErr_Clear();
  }
  return 0;
} // end of tosframe_main




int concat_main(std::string & _output_directory, std::string & _prefix) {
  
  std::string filename;
  std::vector<std::string> list_filenames; 
  
  std::string output_directory = fileio::convert_to_generic(_output_directory);
  
  boost::uuids::uuid file_prefix = boost::uuids::random_generator()();
  std::stringstream ss;
  
  // This works in windows!!
  if(_prefix == "none"){
    ss << output_directory << "/" << file_prefix << ".frame_idx";
  }
  else {
    ss << output_directory << "/" << _prefix << ".frame_idx";
  }

  while(std::getline(std::cin,filename)) { 
    filename.erase(std::remove(filename.begin(), filename.end(), '['), filename.end());
    filename.erase(std::remove(filename.begin(), filename.end(), ']'), filename.end());
    list_filenames.push_back(filename);
  }
  
  if(list_filenames.size() == 0){
    log_and_throw_io_failure("There is no sframe available to concatenate.");  
    return -1;

  }
  
  sframe append_sframe(list_filenames[0]);
  for(int index=1;index<list_filenames.size();index++) { 
      auto sframe_ptr = std::make_shared<sframe>(list_filenames[index]);
      append_sframe = append_sframe.append(*sframe_ptr); 
  }

  //save the appended sframe
  std::string index_str = ss.str();
  sframe_save_weak_reference(append_sframe,index_str);
  //stdout the filepath for the output appended sframe 
  std::cout << index_str << std::endl;
  return 0;
}


/**
 * tordd_main() reads from stdin a range [row_start,row_end) and output
 * a new sframe that only contains rows from input sframe with their indices i inside
 * the range row_start <= i < row_end. 
 *
 **/
int tordd_main(std::string & _output_directory, size_t & numPartitions, size_t & partId) {
  
  std::shared_ptr<sframe> sframe_ptr;
  std::size_t found = _output_directory.find(".frame_idx");
  if (found!=std::string::npos) { 
    sframe_ptr = std::make_shared<sframe>(_output_directory);
  } else {
    dir_archive dirarc;
    dirarc.open_directory_for_read(_output_directory);
    std::string content_value;
    if (dirarc.get_metadata("contents", content_value) == false ||
        content_value != "sframe") {
          log_and_throw_io_failure("Archive does not contain an SFrame");
    }
    std::string prefix = dirarc.get_next_read_prefix();
    sframe_ptr = std::make_shared<sframe>(prefix + ".frame_idx");
  }  

  auto sframe_len = sframe_ptr->size();
  const std::vector<std::string>& column_names = sframe_ptr->column_names();
  auto partition_size = sframe_len/numPartitions;
  if(partId < (sframe_len % numPartitions)) { 
    partition_size +=1;
  }
  size_t row_start = partition_size * partId; 
  size_t row_end = row_start + partition_size;
  if(row_end > sframe_len)
    row_end = sframe_len;
 
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
        //if (row[i].get_type() == flex_type_enum::DATETIME) { 
        //  d[lambda::PyObject_FromFlex(flex_string(column_names[i]))] = lambda::PyObject_FromFlex(flex_string(row[i]));
        //}
        //else { 
          d[lambda::PyObject_FromFlex(flex_string(column_names[i]))] = lambda::PyObject_FromFlex(row[i]);
        //} 
      } 
      python::object pickle_dict = python::object(pickle.attr("dumps")(d,python::object(python::handle<>(PyLong_FromLong(2)))));
      
      //python::object encoded_obj = python::object(base64.attr("b64encode")(pickle_dict));
      std::string stringPickle =  python::extract<std::string>(pickle_dict);
      write_msg(stringPickle.c_str(),stringPickle.size());
      /// good for debugging 
      /*std::cout << stringPickle.length() << "\n";
      for (char c: stringPickle) {
        std::cout << int(c) << "\t";
      }
      std::cout << "\n";*/
      //std::cout << stringPickle << std::endl; 
    }
  }
  return 0;
}


void print_help(std::string program_name, bpo::options_description& desc) {
  std::cerr << "Usage of " << program_name << std::endl
            << desc << std::endl;
}


int main(int argc, char **argv) {
  std::string program_name = argv[0];
  std::string output_directory;
  std::string prefix;
  std::string encoding;
  std::string rdd_type;
  std::string mode;
  size_t numPartitions;
  size_t partId;

  bpo::options_description desc("Program options for the spark_unity binary.");
  bpo::variables_map vm; 

  desc.add_options()
    ("help", "Print this help message")
    ("mode", bpo::value<std::string>(&mode)->required(), 
     "tosframe|tordd|concat")
    ("outputDir", bpo::value<std::string>(&output_directory)->required(), 
      "The output directory to save the result.")
    ("prefix", bpo::value<std::string>(&prefix), 
      "The output name for the final SFrame.")
    ("encoding", bpo::value<std::string>(&encoding), 
     "The serialization format of the standard input bytes.")
    ("type", bpo::value<std::string>(&rdd_type), 
     "dataframe|rdd")
    ("numPartitions", bpo::value<size_t>(&numPartitions), 
     "Number of partitions of the output rdd in tordd mode.")
    ("partId", bpo::value<size_t>(&partId), 
     "Partition index of the output rdd in tordd mode.");

  // // Parse command line options
  // graphlab::command_line_options clopts("Spark Unity Command Line Options!");
  // clopts.attach_option("outputDir",output_directory, 
  //                     "The output directory to save the result.");
  // clopts.attach_option("prefix",prefix, 
  //                     "The output name for the final SFrame.");
  // clopts.attach_option("encoding",encoding, 
  //                     "The serialization format of the standard input bytes.");
  // clopts.attach_option("type",rdd_type, 
  //                     "dataframe|rdd");
  // clopts.attach_option("mode",mode, 
  //                     "tosframe|tordd|concat");
  // clopts.attach_option("numPartitions",numPartitions, 
  //                     "Number of partitions of the output rdd in tordd mode.");
  // clopts.attach_option("partId",partId, 
  //                     "Partition index of the output rdd in tordd mode.");
  try {
    bpo::store(bpo::command_line_parser(argc, argv).options(desc).run(), vm); 
    bpo::notify(vm);    
  } catch(std::exception& error) {
    std::cerr << "Invalid syntax:\n"
              << "\t" << error.what()
              << "\n\n" << std::endl
              << "Description:"
              << std::endl;
    print_help(program_name, desc);
    return EXIT_FAILURE;
  }

  if(vm.count("help")) {
    print_help(program_name, desc);
    return EXIT_SUCCESS;
  }

  logprogress_stream << "mode: " << mode << " type: " << rdd_type 
                     << " encoding: " << encoding << std::endl;

  if(mode == "tosframe") { 
    if (vm.count("encoding") == 0 || vm.count("type") == 0) {
      std::cerr << "Encoding and type must be set for tosframe" << std::endl;
      return EXIT_FAILURE;
    }    
    return tosframe_main(output_directory, encoding, rdd_type);
  } else if(mode == "concat") {
    return concat_main(output_directory, prefix);
  } else if(mode == "tordd") { 
    if (vm.count("partId") == 0 || vm.count("numPartitions") == 0) {
      std::cerr << "partId and numPartitions must be set for mode tordd" 
                << std::endl;
      return EXIT_FAILURE;
    }
    return tordd_main(output_directory, numPartitions, partId);
  } else { 
    std::cerr << "Invalid mode type: " << mode << std::endl;
    print_help(program_name, desc);
    return EXIT_FAILURE;
  }
}
