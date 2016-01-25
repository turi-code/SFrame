/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <lambda/pylambda.hpp>
#include <lambda/python_api.hpp>
#include <lambda/python_import_modules.hpp>
#include <lambda/python_thread_guard.hpp>
#include <lambda/pyflexible_type.hpp>
#include <sframe/sarray.hpp>
#include <sframe/sframe.hpp>
#include <sframe/sframe_rows.hpp>
#include <fileio/fs_utils.hpp>
#include <util/cityhash_gl.hpp>
#include <shmipc/shmipc.hpp>

namespace graphlab {

namespace lambda {

  namespace python = boost::python;

  pylambda_evaluator::~pylambda_evaluator() {
    if (m_shared_memory_listener.active()) {
      m_shared_memory_thread_terminating = true;
      m_shared_memory_listener.join();
    }
  }

  size_t pylambda_evaluator::make_lambda(const std::string& pylambda_str) {
    python_thread_guard py_thread_guard;
    auto status = fileio::get_file_status(pylambda_str);
    if (status == fileio::file_status::DIRECTORY) {
      try {
        // Does python::object(python::handle<>(lambda_filename)) decref this?
        PyObject* lambda_filename = 
            PyString_FromStringAndSize(pylambda_str.c_str(), pylambda_str.size());
        size_t hash_key = hash64(pylambda_str.c_str(), pylambda_str.size());
        auto unpickler = py_gl_pickle.attr("GLUnpickler")(python::object(python::handle<>(lambda_filename)));
        m_lambda_hash[hash_key] = new python::object(unpickler.attr("load")());
        return hash_key;
      } catch (python::error_already_set const& e) {
        std::string error_string = parse_python_error();
        throw(error_string);
      }
    } else {
      try {
        PyObject* lambda_bytes = PyByteArray_FromStringAndSize(pylambda_str.c_str(), pylambda_str.size());
        size_t hash_key = hash64(pylambda_str.c_str(), pylambda_str.size());
        m_lambda_hash[hash_key] = new python::object((py_pickle.attr("loads")(python::object(python::handle<>(lambda_bytes)))));
        logstream(LOG_DEBUG) << "make lambda" << hash_key << std::endl;
        return hash_key;
      } catch (python::error_already_set const& e) {
        std::string error_string = parse_python_error();
        throw(error_string);
      }
    }
  }

  void pylambda_evaluator::release_lambda(size_t lambda_hash) {
    python_thread_guard py_thread_guard;
    logstream(LOG_DEBUG) << "release lambda" << lambda_hash << std::endl;
    auto lambda_obj = m_lambda_hash.find(lambda_hash);

    if (lambda_obj == m_lambda_hash.end()) {
      throw("Cannot find the lambda hash to release " + std::to_string(lambda_hash));
    }

    if (m_current_lambda_hash == lambda_hash) {
      m_current_lambda_hash = (size_t)(-1);
      m_current_lambda = NULL;
    }

    delete m_lambda_hash[lambda_hash];
    m_lambda_hash.erase(lambda_hash);

    // run gc to reclaim heap
    py_gc.attr("collect")();
  }

  flexible_type pylambda_evaluator::eval(size_t lambda_hash, const flexible_type& arg) {
    set_lambda(lambda_hash);
    try {
        python::object input = PyObject_FromFlex(arg);
        python::object output = m_current_lambda->operator()(input);
        return PyObject_AsFlex(output);
    } catch (python::error_already_set const& e) {
      std::string error_string = parse_python_error();
      throw(error_string);
    } catch (std::exception& e) {
      throw(std::string(e.what()));
    } catch (...) {
      throw("Unknown exception from python lambda evaluation.");
    }
  }

  /**
   * Bulk version of eval.
   */
  std::vector<flexible_type> pylambda_evaluator::bulk_eval(
    size_t lambda_hash,
    const std::vector<flexible_type>& args,
    bool skip_undefined,
    int seed) {

    python_thread_guard py_thread_guard;

    set_lambda(lambda_hash);

    py_set_random_seed(seed);

    std::vector<flexible_type> ret(args.size());
    size_t i = 0;
    for (const auto& x : args) {
      if (skip_undefined && x == FLEX_UNDEFINED) {
        ret[i++] = FLEX_UNDEFINED;
      } else {
        ret[i++] = eval(lambda_hash, x);
      }
    }
    return ret;
  }

  std::vector<flexible_type> pylambda_evaluator::bulk_eval_rows(
    size_t lambda_hash,
    const sframe_rows& rows,
    bool skip_undefined,
    int seed) {

    python_thread_guard py_thread_guard;

    set_lambda(lambda_hash);

    py_set_random_seed(seed);

    std::vector<flexible_type> ret(rows.num_rows());
    size_t i = 0;
    for (const auto& x : rows) {
      if (skip_undefined && x[0] == FLEX_UNDEFINED) {
        ret[i++] = FLEX_UNDEFINED;
      } else {
        ret[i++] = eval(lambda_hash, x[0]);
      }
    }
    return ret;
  }


  /**
   * Bulk version of eval_dict.
   */
  std::vector<flexible_type> pylambda_evaluator::bulk_eval_dict(
    size_t lambda_hash,
    const std::vector<std::string>& keys,
    const std::vector<std::vector<flexible_type>>& values,
    bool skip_undefined,
    int seed) {

    python_thread_guard py_thread_guard;

    set_lambda(lambda_hash);
    py_set_random_seed(seed);
    std::vector<flexible_type> ret(values.size());
    python::dict input;
    size_t cnt = 0;
    try {
        for (const auto& val: values) {
          PyDict_UpdateFromFlex(input, keys, val);
          python::object output = m_current_lambda->operator()(input);
          PyObject_AsFlex(output, ret[cnt]);
          cnt++;
        }
    } catch (python::error_already_set const& e) {
      std::string error_string = parse_python_error();
      throw(error_string);
    } catch (std::exception& e) {
      throw(std::string(e.what()));
    } catch (...) {
      throw("Unknown exception from python lambda evaluation.");
    }
    return ret;
  }

  std::vector<flexible_type> pylambda_evaluator::bulk_eval_dict_rows(
    size_t lambda_hash,
    const std::vector<std::string>& keys,
    const sframe_rows& rows,
    bool skip_undefined,
    int seed) {

    python_thread_guard py_thread_guard;

    set_lambda(lambda_hash);
    py_set_random_seed(seed);
    std::vector<flexible_type> ret(rows.num_rows());
    python::dict input;
    size_t cnt = 0;
    try {
      std::vector<flexible_type> row(rows.num_columns());
      for (const auto& row_iter: rows) {
        for (size_t j = 0; j < row.size(); ++j)
          row[j] = row_iter[j];
        PyDict_UpdateFromFlex(input, keys, row);
        python::object output = m_current_lambda->operator()(input);
        PyObject_AsFlex(output, ret[cnt]);
        cnt++;
      }
    } catch (python::error_already_set const& e) {
      std::string error_string = parse_python_error();
      throw(error_string);
    } catch (std::exception& e) {
      throw(std::string(e.what()));
    } catch (...) {
      throw("Unknown exception from python lambda evaluation.");
    }
    return ret;
  }


  void pylambda_evaluator::set_lambda(size_t lambda_hash) {
    if (m_current_lambda_hash == lambda_hash) return;

    auto lambda_obj = m_lambda_hash.find(lambda_hash);
    if (lambda_obj == m_lambda_hash.end()) {
      throw("Cannot find a lambda handle that is value " + std::to_string(lambda_hash));
    }

    m_current_lambda = m_lambda_hash[lambda_hash];
    m_current_lambda_hash = lambda_hash;
  }

  std::vector<flexible_type> 
  pylambda_evaluator::bulk_eval_rows_serialized(const char* ptr, size_t len) {
    iarchive iarc(ptr, len);
    char c;
    iarc >> c;
    if (c == (char)bulk_eval_serialized_tag::BULK_EVAL_ROWS) {
      size_t lambda_hash;
      sframe_rows rows;
      bool skip_undefined;
      int seed;
      iarc >> lambda_hash >> rows >> skip_undefined >> seed;
      return bulk_eval_rows(lambda_hash, rows, skip_undefined, seed);
    } else if (c == (char)bulk_eval_serialized_tag::BULK_EVAL_DICT_ROWS) {
      size_t lambda_hash;
      std::vector<std::string> keys;
      sframe_rows values;
      bool skip_undefined;
      int seed;
      iarc >> lambda_hash >> keys >> values >> skip_undefined >> seed;
      return bulk_eval_dict_rows(lambda_hash, keys, values, skip_undefined, seed);
    } else {
      logstream(LOG_FATAL) << "Invalid serialized result" << std::endl;
    }
  }

  std::string pylambda_evaluator::initialize_shared_memory_comm() {
    if (m_shared_memory_server) {
      if (!m_shared_memory_listener.active()) {
        m_shared_memory_listener.launch(
            [=]() {
            while(!m_shared_memory_server->wait_for_connect(3)) {
              if (m_shared_memory_thread_terminating) return;
            }
            char* receive_buffer = nullptr;
            size_t receive_buffer_length = 0;  
            size_t message_length = 0;
            char* send_buffer = nullptr;
            size_t send_buffer_length= 0 ;
            while(1) {
              bool has_data = 
                  shmipc::large_receive(*m_shared_memory_server,
                                        &receive_buffer, 
                                        &receive_buffer_length, 
                                        message_length, 
                                        3 /* timeout */);
              if (!has_data) {
                if (m_shared_memory_thread_terminating) break;
                else continue;
              } else {
                oarchive oarc;
                oarc.buf = send_buffer;
                oarc.len = send_buffer_length;
                try {
                  auto ret = bulk_eval_rows_serialized(receive_buffer, message_length);
                  oarc << (char)(1) << ret;
                } catch (std::string& s) {
                  oarc << (char)(0) << s;
                } catch (const char* s) {
                  oarc << (char)(0) << std::string(s);
                } catch (...) {
                  oarc << (char)(0) << std::string("Unknown Runtime Exception");
                }
                shmipc::large_send(*m_shared_memory_server,
                                   oarc.buf,
                                   oarc.off);
                send_buffer = oarc.buf;
                send_buffer_length = oarc.len;
              }
            }
            if (receive_buffer) free(receive_buffer);
            if (send_buffer) free(send_buffer);
            });
      }
      return m_shared_memory_server->get_shared_memory_name();
    } else {
      return "";
    }
  }
} // end of namespace lambda
} // end of namespace graphlab
