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
#include <cmath>
#include <boost/python.hpp>
#include <lambda/pylambda.hpp>
#include <lambda/python_api.hpp>
#include <lambda/python_thread_guard.hpp>
#include <fstream>

using namespace std;
using namespace boost::python;

int main(int argc, char** argv) {
  graphlab::lambda::init_python(argc, argv);
  graphlab::lambda::python_thread_guard py_thread_guard;
  try {
    graphlab::lambda::pylambda_evaluator evaluator;
    size_t lambda_hash = evaluator.make_lambda("123");
    evaluator.bulk_eval(lambda_hash, {1,2,3}, false, 0);
  } catch (error_already_set const& e) {
    std::string error_string = graphlab::lambda::parse_python_error();
    std::cerr << error_string << std::endl;
  }
  return 0;
}
