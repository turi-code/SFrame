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
#include<fileio/run_aws.hpp>
#include<stdlib.h>
#include<string>
#include<iostream>

void print_help(char** argv) {
  std::cout << "This program wraps the awscli command \"aws\".\n";
  std::cout << "Usage: \n";
  std::cout << argv[0] << " s3 ls [src]\n";
  std::cout << argv[0] << " s3 cp [src] [dst]\n";
  std::cout << "The environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY will be used if available\n";
  std::cout << "\n";
}


int main(int argc, char** argv) {
  if (argc == 1) {
    print_help(argv);
    return 0;
  }

  std::vector<std::string> arglist;
  for (size_t i = 1; i < argc; ++i) {
    arglist.push_back(std::string(argv[i]));
  }
  auto ret = graphlab::fileio::run_aws_command(arglist,
                                    getenv("AWS_ACCESS_KEY_ID"),
                                    getenv("AWS_SECRET_ACCESS_KEY"));
  std::cout << ret << std::endl;
  exit(0);
}
