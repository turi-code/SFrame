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
#include <tuple>
#include <fstream>
#include <fileio/curl_downloader.hpp>

std::ofstream fout;
  
size_t write_callback(void *buffer, size_t size, size_t nmemb, void *stream) {
  fout.write((char*)buffer, size * nmemb);
  return size * nmemb;
}
 

int main(int argc, char** argv) {
  if (argc < 2) {
    std::cout << "Usage: " << argv[0] << " [webpage]\n";
    exit(0);
  }
  int status; bool is_temp; std::string filename;
  std::tie(status, is_temp, filename) = graphlab::download_url(argv[1]);
  if (status != 0) {
    std::cout << "Failed to download file\n";
  } else if (is_temp) {
    std::cout << "Temporary file saved to " << filename << "\n";
  } else {
    std::cout << "Is local file at " << filename << "\n";
  }
}

