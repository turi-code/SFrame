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
#include <fault/query_object_server_process.hpp>

using namespace libfault;
class echo_server: public query_object {
 private:
   bool is_master;
   size_t counter;
 public:
   echo_server() {
     counter = 0;
   }
  void query(char* msg, size_t msglen,
             char** outreply, size_t *outreplylen) {
    (*outreply) = (char*)malloc(msglen);
    (*outreplylen) = msglen;
    if (is_master) {
      std::cout << "Master: " << counter << " ";
    } else {
      std::cout << "Slave: " << counter << " ";
    }
    std::cout.write(msg, msglen);
    std::cout << "\n";
    memcpy(*outreply, msg, msglen);
  }

  bool update(char* msg, size_t msglen,
              char** outreply, size_t *outreplylen) {
    (*outreply) = (char*)malloc(msglen);
    (*outreplylen) = msglen;

    if (is_master) {
      std::cout << "Master: " << counter << " ";
    } else {
      std::cout << "Slave: " << counter << " ";
    }
    std::cout.write(msg, msglen);
    std::cout << "\n";


    memcpy(*outreply, msg, msglen);
    ++counter;
    return true;
  }

  void upgrade_to_master() {
    std::cout << "Upgrade to master\n";
    is_master = true;
  }

  void serialize(char** outbuf, size_t *outbuflen) {
    (*outbuf) = (char*)malloc(sizeof(size_t));
    (*(size_t*)(*outbuf)) = counter;
    (*outbuflen ) = sizeof(size_t);
  }
  void deserialize(const char* buf, size_t buflen) {
    assert(buflen == sizeof(size_t));
    counter = (*(size_t*)(buf));
  }

  static query_object* factory(std::string objectkey,
                               std::vector<std::string> zk_hosts,
                               std::string zk_prefix,
                               uint64_t create_flags) {
    // this is the factory function which makes query objects.
    echo_server* es = new echo_server;
    es->is_master = (create_flags & QUERY_OBJECT_CREATE_MASTER);
    return es;
  }


};


int main(int argc, char** argv) {
  query_main(argc, argv, echo_server::factory);
}
