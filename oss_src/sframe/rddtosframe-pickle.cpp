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
#include <flexible_type/flexible_type.hpp>
#include <sframe/sframe.hpp>
#include <sframe/csv_line_tokenizer.hpp>
#include <sframe/sframe_constants.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/concepts.hpp>
#include <boost/iostreams/operations.hpp> 
#include <boost/iostreams/device/file.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/python.hpp>
#include <lambda/pyflexible_type.hpp>

/**
 * rddtosframe-pickle.cpp reads rdd rows from the stdin, does type inference, and save 
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

using namespace graphlab;
namespace python = boost::python;

inline void utf8_decode(const std::string& val, 
    std::string& output, size_t& output_len) {
  if (output.size() < val.size()) {
    output.resize(val.size());
  }
  char* cur_out = &(output[0]);
  // loop through the input string
  for (size_t i = 0; i < val.size(); ++i) {
    char c = val[i];
    if((c & 0x80) == 0) { 
      (*cur_out++) = c; 
    } else { 
      if((c & 0xC2) != 0xC2) {
        log_and_throw("utf8 format is wrong, 110xxxxx is required " + std::to_string(int(c)) + " is given"); 
      }
      char outChar = (c << 6);
      if(i < val.size() - 1) { 
        outChar = (outChar | val[i+1]);
        i++;
      } else { 
        log_and_throw("utf8 format is wrong, 10xxxxxx is required");
      }
      (*cur_out++) = outChar;
    }  
  }
  output_len = cur_out - &(output[0]);
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
void infer_schema(const flexible_type & row,bool is_schemardd,
    std::vector<std::string> & column_names,
    std::vector<flex_type_enum> & column_types) { 

  size_t ncols = 1; 
  if(is_schemardd && row.get_type() == flex_type_enum::DICT) { 
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
  } else if(is_schemardd && row.get_type() == flex_type_enum::LIST) { 
    flex_list rec = row.get<flex_list>();
    ncols = rec.size();
    column_names.resize(ncols);
    column_types.resize(ncols);
    for(size_t i = 0;i < ncols; ++i) {
      column_names[i] = "X"+ std::to_string(i + 1);;
      column_types[i] = rec[i].get_type();
      if(column_types[i] == flex_type_enum::UNDEFINED) {
        column_types[i] = flex_type_enum::STRING;
      }
    }
  } else if(is_schemardd && row.get_type() == flex_type_enum::VECTOR) { 
    flex_vec vec = row.get<flex_vec>();
    ncols = vec.size();
    column_names.resize(ncols);
    column_types.resize(ncols);
    for(size_t i = 0;i < ncols; ++i) {
      column_names[i] = "X"+ std::to_string(i + 1);;
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
 * Initialize tokens. Tokens is a container where column values with 
 * correct types will be saved.
 */
void initialize_tokens(const std::vector<flex_type_enum> & column_types,std::vector<flexible_type> & tokens) { 
  size_t ncols = column_types.size();
  tokens = std::vector<flexible_type>(ncols,flex_string());
  for(size_t i = 0;i < ncols; ++i) {
    if(column_types[i] != flex_type_enum::STRING)
      tokens[i].reset(column_types[i]);
  }
}

void assign_value_tokens(const flexible_type & row,bool is_schemardd,std::vector<flexible_type> & tokens) { 

  size_t ncols = 1;
  if(is_schemardd && row.get_type() == flex_type_enum::DICT) {

    flex_dict dict = row.get<flex_dict>();
    ncols = dict.size();
    for (size_t i = 0;i < ncols; ++i) {
      tokens[i] = dict[i].second;
    }

  } else if (is_schemardd && row.get_type() == flex_type_enum::VECTOR) { 
    flex_vec vect = row.get<flex_vec>();
    ncols = vect.size();

    for (size_t i = 0;i < ncols; ++i) {
      tokens[i] = vect[i];
    }
  } else if (is_schemardd && row.get_type() == flex_type_enum::LIST) { 

    flex_list rec = row.get<flex_list>();
    ncols = rec.size();

    for (size_t i = 0;i < ncols; ++i) {
      tokens[i] = rec[i];
    }       
  } else { 
    tokens[0] = row;
  }    
}
int main(int argc, char **argv) {
  try {
    /*** parsing input arguments ***/  
    if(argc != 4) {
      /** 
       * argv[1]: Specifies the directory to save the output SFrame. 
       * argv[2]: Determines if we are reading 'pickle' or 'batch' serialized items.
       * argv[3]: Determines if we are reading from rdd or schema-rdd. 
       */
      std::cerr << "Usage: " << argv[0] << " <output directory> <batch-or-pickle> <rdd-or-schemardd>" <<  std::endl;
      return -1;
    }
    bool is_batch_mode = false;
    bool is_schemardd = false;
    std::string output_directory =
      fileio::convert_to_generic(std::string(argv[1]));
    boost::iostreams::filtering_istream fin;
    std::vector<std::string> column_names;
    std::vector<flex_type_enum> column_types;
    std::vector<flexible_type> tokens;
    
    if (std::string(argv[2]) == "batch") { 
      is_batch_mode = true;  
    }
    if (std::string(argv[3]) == "schemardd") { 
      is_schemardd = true;
    }
    /// finish parsing 

    fin.push(std::cin);  
    
    /*** extract the first line ***/
    if (!fin.good()) {
      log_and_throw("Fail reading from standard input");
    }

    std::string cur_line;
    if(!std::getline(fin,cur_line)) { 
      return 0;
    }
    flexible_type first_row;
    flex_list first_batch;
    /// Initialze the boost.python and import necessary modules.  
    Py_Initialize(); 
    lambda::import_modules();
    python::object pickle = python::import("cPickle");
    python::object base64 = python::import("base64");
    python::object first_line_obj = python::object(python::handle<>(PyString_FromString(cur_line.c_str())));   
    python::object decoded_obj = python::object(base64.attr("b64decode")(first_line_obj));
    python::object unpickle_obj = python::object(pickle.attr("loads")(decoded_obj));
    flexible_type first_val = lambda::PyObject_AsFlex(unpickle_obj);
    /**
     * NOTE: If we are on the 'batch' mode then "PyObject_AsFlex()" either 
     * returns flex_type_enum::VECTOR for an array of numeric values  
     * or it returns flex_type_enum::LIST for non-numeric values. 
     * We do NOT want to call first_val.get<flex_list>() for the former 
     * condition which has unnecessary copy from the vector to a list. 
     * If we are on the 'pickle' mode then we wrap the single flexible_type 
     * first_val in a vector of flexible_type and assign it to first_bach.
     */
    if(is_batch_mode && first_val.get_type() == flex_type_enum::VECTOR) { 
      first_batch = first_val;
    }
    else if(is_batch_mode && first_val.get_type() == flex_type_enum::LIST) { 
      first_batch = first_val.get<flex_list>();
    }
    else { 
      first_batch = std::vector<flexible_type>{first_val};
    }
    first_row = first_batch[0];
    /// finish extracting the first line

    /// infer the schema for the sframe from the first row.
    /// TODO: perhaps we should do it from the first n rows.
    infer_schema(first_row,is_schemardd,column_names,column_types);
    initialize_tokens(column_types,tokens);
    /// Create the SFrame to write to
    boost::uuids::uuid file_prefix = boost::uuids::random_generator()();
    std::stringstream ss;
    ss << output_directory << "/" << file_prefix << ".frame_idx";
    sframe frame;
    frame.open_for_write(column_names, column_types, "", 1, false);

    /// Iterate over all the input lines and write it to the ouput sframe. 
    auto it_out = frame.get_output_iterator(0);
    flexible_type row;
    flex_list batch;
    do {
      python::object curVal_obj = python::object(python::handle<>(PyString_FromString(cur_line.c_str())));
      python::object decoded_obj = python::object(base64.attr("b64decode")(curVal_obj));
      python::object unpickle_obj = python::object(pickle.attr("loads")(decoded_obj));
      flexible_type val = lambda::PyObject_AsFlex(unpickle_obj);

      if(val.get_type() == flex_type_enum::VECTOR && is_batch_mode) { 
        flex_vec vect_batch = val.get<flex_vec>();
        for(size_t index = 0 ; index < vect_batch.size() ; index++) { 
          tokens[0] = vect_batch[index];
          *it_out = tokens;
          ++it_out;
        }
        continue;
      }
      else if(val.get_type() == flex_type_enum::LIST && is_batch_mode) {
        batch = val.get<flex_list>();
      }
      else {  
        batch = std::vector<flexible_type>{val};
      }
      
      for(size_t index = 0 ; index < batch.size() ; index++) {  
        row = batch[index];
        assign_value_tokens(row,is_schemardd,tokens);
        *it_out = tokens;
        ++it_out;
      }
    } while (std::getline(fin, cur_line)); 
    /// end of iteration. 


    if(frame.is_opened_for_write()) {
      frame.close();
    }
    std::string index_str = ss.str();
    std::cout << index_str << std::endl;
    frame.save(index_str);

  } catch (python::error_already_set const& e) {
    if (PyErr_Occurred()) {
      std::string msg = handle_pyerror(); 
      std::cerr << "GRAPHLAB PYTHON-ERROR: " << msg << std::endl;
      throw(msg);
    }
    //bool py_exception = true;
    python::handle_exception();
    PyErr_Clear();
  }
  return 0;

}
