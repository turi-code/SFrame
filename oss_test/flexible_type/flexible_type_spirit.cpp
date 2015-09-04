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
#include <flexible_type/flexible_type_spirit_parser.hpp>
using namespace graphlab;
using namespace boost::spirit;

int main(int argc, char** argv) {
  std::string s;
  std::string delimiter = ",";
  if (argc == 2) delimiter = argv[1];
  flexible_type_parser parser(delimiter);
    std::string line;
    while(std::getline(std::cin, line)) {
      if (!s.empty()) s += "\n";
      s += line;
    }
    const char* c = s.c_str();
    std::pair<flexible_type, bool> ret = parser.general_flexible_type_parse(&c, s.length());
    if (ret.second) {
      std::cout << flex_type_enum_to_name(ret.first.get_type()) << ":";
      std::cout << ret.first << "\n";
      std::cout << "Remainder: " << c << "\n";
    } else {
      std::cout << "Failed Parse\n";
    }


}

