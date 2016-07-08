/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <lambda/pylambda_function.hpp>
#include <lambda/lambda_master.hpp>
#include <sframe/sframe_rows.hpp>
#include <fileio/file_ownership_handle.hpp>

namespace graphlab {

namespace lambda {

pylambda_function::pylambda_function(const std::string& lambda_str,
                                     bool delete_pickle_files_on_destruction) {
  lambda_hash = lambda::lambda_master::get_instance().make_lambda(lambda_str);

  if (fileio::get_file_status(lambda_str) == fileio::file_status::DIRECTORY &&
      delete_pickle_files_on_destruction) {
     m_pickle_file_handle = 
         std::make_shared<fileio::file_ownership_handle>(lambda_str,
                                                         true, // delete on destruction
                                                         true); // recursive delete
  }
}

pylambda_function::~pylambda_function() {
  lambda::lambda_master::get_instance().release_lambda(lambda_hash);
}

//// Options
void pylambda_function::set_skip_undefined(bool value) { 
  skip_undefined = value;
}

void pylambda_function::set_random_seed(int value) {
  random_seed = value;
}

//// Evaluating Interface 

/* One to one */
void pylambda_function::eval(const sframe_rows& rows,
                             std::vector<flexible_type>& out) {
  lambda::lambda_master::get_instance().bulk_eval(lambda_hash, rows, out, skip_undefined, random_seed);
};

/* Many to one */
void pylambda_function::eval(const std::vector<std::string>& keys, 
                             const sframe_rows& rows,
                             std::vector<flexible_type>& out) {
  lambda::lambda_master::get_instance().bulk_eval(lambda_hash, keys, rows, out, skip_undefined, random_seed);
}

} // end of lambda
} // end of graphlab
