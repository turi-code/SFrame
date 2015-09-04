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
#include <iostream>
#include <boost/program_options.hpp>
#include <fileio/s3_api.hpp>

namespace po = boost::program_options;      

int main(int argc, char** argv) {
  std::string local;
  std::string s3url;
  bool download = false, upload = false;
  // command line options parsing.
  po::options_description desc("s3test program");
  po::variables_map vm;
  desc.add_options()
      ("help", "Print this help message.")
      ("download", "If set, will download from remote to local. "
                   "Cannot be specified together with \"upload\"")
      ("upload", "If set, will upload from local to remote. "
                 "Cannot be specified together with \"download\"")
      ("local", 
       po::value<std::string>(&local),
       "Local file to upload/download.")
      ("s3url", 
       po::value<std::string>(&s3url),
       "S3URL. Must be of the form "
       "s3://[access_key_id]:[secret_key]:[bucket]/[object_name]");

  try {
    po::command_line_parser parser(argc, argv);
    parser.options(desc);
    po::parsed_options parsed = parser.run();
    po::store(parsed, vm);
    po::notify(vm);
  } catch(po::error error) {
    std::cout << "Invalid syntax:\n"
              << "\t" << error.what()
              << "\n\n" << std::endl
              << "Description:"
              << std::endl;
    std::cout << desc << std::endl;
    return 1;
  }
  // print help
  if(vm.count("help")) {
    std::cout << desc << std::endl;
    return 0;
  }

  // look for the --download and --upload
  if(vm.count("download")) download = true;
  if(vm.count("upload")) upload = true;

  // check that only one of upload/download is specified
  if (download && upload) {
    std::cout << "Both Upload and Download cannot be specified at the same time"
              << std::endl;
    return 1;
  } else if (!download && !upload) {
    std::cout << "Either upload or download has to be specified"
              << std::endl;
    return 1;
  }
  std::future<std::string> ret;
  if (download) {
    ret = webstor::download_from_s3(s3url, local);
  } else if (upload) {
    ret = webstor::upload_to_s3(local, s3url);
  }
  std::cout << "Command issued" << std::endl;
  std::string response = ret.get();
  if (response.empty()) std::cout << "Command Success" << std::endl;
  else std::cout << "Error: " << response << std::endl;

}
