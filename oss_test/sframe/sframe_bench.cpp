/*
* Copyright (C) 2016 Turi
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
#include <iostream>
#include <sframe/sframe.hpp>
#include <sframe/parallel_csv_parser.hpp>
#include <fileio/temp_files.hpp>
#include <timer/timer.hpp>
using namespace graphlab;

int main(int argc, char** argv) {
  if (argc != 2) {
    std::cout << argv[0] << " [csv file]\n";
    std::cout << "file must contain headers, and be comma separated\n";
    return 0;
  }
  std::string prefix = get_temp_name();
  timer ti;
  csv_line_tokenizer tokenizer;
  tokenizer.delimiter = ',';
  tokenizer.init();
  sframe frame;
  frame.init_from_csvs(argv[1],
                       tokenizer,
                       true,   // header
                       true,   // continue on failure
                       false,  // do not store errors
                       std::map<std::string, flex_type_enum>());

  std::cout << "CSV file parsed in " << ti.current_time() << " seconds\n";
  std::cout << "Columns are: \n";
  for (size_t i = 0; i < frame.num_columns(); ++i) {
    std::cout << frame.column_name(i) << "\n";
  }
  std::cout << frame.num_rows() << " rows\n";
}
