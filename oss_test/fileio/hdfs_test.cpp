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
/**  
 * Copyright (c) 2009 Carnegie Mellon University. 
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS
 *  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied.  See the License for the specific language
 *  governing permissions and limitations under the License.
 *
 * For more about this software visit:
 *
 *      http://www.graphlab.ml.cmu.edu
 *
 */


#include <iostream>
#include <vector>
#include <fileio/hdfs.hpp>

int main(int argc, char **argv) {

    if (argc < 3) {
        std::cout << "Usage: " << argv[0] << " <hostname> <port> " << std::endl;
        std::cout << std::endl;
        std::cout << "Note: CLASSPATH and LD_LIBRARY_PATH should be set correctly." << std::endl;
        std::cout << "export LD_LIBRARY_PATH=/usr/lib/jvm/default-java/jre/lib/amd64/server:$LD_LIBRARY_PATH" << std::endl;
        std::cout << "From active graphlab virtualenv, run python gen-classpath.py (if not in debug dir then look in src/fileio)" << std::endl;
        exit(1);
    }

    std::string hostname = std::string(argv[1]);
    size_t port = atoi(argv[2]);

    {
        global_logger().set_log_level(LOG_INFO);
        graphlab::hdfs hdfs(hostname, port);
        const bool write = true;
        graphlab::hdfs::fstream file(hdfs, "/user/rajat/test.txt", write);
        file.good();
        file << "Hello World\n";
        file.close();
        std::vector<std::string> files = hdfs.list_files("/user/rajat/test.txt");
        for(size_t i = 0; i < files.size(); ++i) {
            std::cout << files[i] << std::endl;
        }
    }

/*
  {
    graphlab::hdfs hdfs(hostname, port);
    graphlab::hdfs::fstream file(graphlab::hdfs::get_hdfs(), "/tmp/joeytest.txt");
    file.good();
    std::string answer;
    std::getline(file, answer);
    file.close();
    std::cout << "contents: " << std::endl;
    std::cout << answer << std::endl;
  }
*/
  std::cout << "Done!" << std::endl;
}
