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
#include <cppipc/common/message_types.hpp>
#include <fault/zmq/zmq_msg_standard_free.hpp>
#include <serialization/serialization_includes.hpp>
namespace cppipc {

void call_message::clear() {
  if (!zmqbodyused) {
    if (!body) free(body);
  } else {
    zmq_msg_close(&bodybuf);
  }
  body = NULL;
  objectid = 0;
  zmqbodyused = false;
}

bool call_message::construct(libfault::zmq_msg_vector& msg) {
  clear();
  if(msg.size() != 4) return false;
  // first block is the object ID
  if (zmq_msg_size(msg.front()) != sizeof(size_t)) return false;
  objectid = *reinterpret_cast<size_t*>(zmq_msg_data(msg.front()));
  msg.pop_front_and_free();

  // second block is the property bag
  char* propertybuf = (char*)zmq_msg_data(msg.front());
  size_t propertybuflen = zmq_msg_size(msg.front());
  graphlab::iarchive iarc(propertybuf, propertybuflen);
  iarc >> properties;
  msg.pop_front_and_free();

  // third block is the function name
  function_name = std::string((const char*)zmq_msg_data(msg.front()), 
                              zmq_msg_size(msg.front()));
  msg.pop_front_and_free();
  zmq_msg_init(&bodybuf);  
  zmq_msg_move(&bodybuf, msg.front());
  body = (char*)zmq_msg_data(&bodybuf);
  bodylen = zmq_msg_size(&bodybuf);
  zmqbodyused = true;
  msg.pop_front_and_free(); // no free this time since we are keeping a pointer
  return true;
}

void call_message::emit(libfault::zmq_msg_vector& msg) {
  assert(zmqbodyused == false);
  // first block is the object ID
  zmq_msg_t* z_objectid = msg.insert_back();
  zmq_msg_init_size(z_objectid, sizeof(size_t));
  (*reinterpret_cast<size_t*>(zmq_msg_data(z_objectid))) = objectid;

  // second block is the property bag
  graphlab::oarchive oarc;
  oarc << properties;
  zmq_msg_t* z_propertybag = msg.insert_back();
  zmq_msg_init_data(z_propertybag, oarc.buf, oarc.off, 
                    libfault::zmq_msg_standard_free, NULL);

  // third block is the function name
  zmq_msg_t* z_function_name = msg.insert_back();
  zmq_msg_init_size(z_function_name, function_name.length());
  memcpy(zmq_msg_data(z_function_name), 
         function_name.c_str(), 
         function_name.length());

  // third block is the serialization body
  zmq_msg_t* z_body = msg.insert_back();

  if (body != NULL) {
    zmq_msg_init_data(z_body, body, bodylen, libfault::zmq_msg_standard_free, NULL);
  }
  
  // we are giving away the body pointer
  body = NULL;
  clear();
}



void reply_message::clear() {
  if (!zmqbodyused) {
    if (!body) free(body);
  } else {
    zmq_msg_close(&bodybuf);
  }
  body = NULL;
  bodylen = 0;
  zmqbodyused = false;
}

bool reply_message::construct(libfault::zmq_msg_vector& msg) {
  clear();
  if(msg.size() != 3) return false;
  // first block is the reply status
  if (zmq_msg_size(msg.front()) != sizeof(reply_status)) return false;
  status = *reinterpret_cast<reply_status*>(zmq_msg_data(msg.front()));
  msg.pop_front_and_free();

  // second block is the property bag
  char* propertybuf = (char*)zmq_msg_data(msg.front());
  size_t propertybuflen = zmq_msg_size(msg.front());
  graphlab::iarchive iarc(propertybuf, propertybuflen);
  iarc >> properties;
  msg.pop_front_and_free();

  // third block is the serialization body
  zmq_msg_init(&bodybuf);  
  zmq_msg_move(&bodybuf, msg.front());
  body = (char*)zmq_msg_data(&bodybuf);
  bodylen = zmq_msg_size(&bodybuf);
  zmqbodyused = true;
  msg.pop_front_and_free(); // no free this time since we are keeping a pointer
  return true;
}

void reply_message::emit(libfault::zmq_msg_vector& msg) {
  assert(zmqbodyused == false);
  // first block is the reply status
  zmq_msg_t* z_status = msg.insert_back();
  zmq_msg_init_size(z_status, sizeof(reply_status));
  (*reinterpret_cast<reply_status*>(zmq_msg_data(z_status))) = status;

  // second block is the property bag
  graphlab::oarchive oarc;
  oarc << properties;
  zmq_msg_t* z_propertybag = msg.insert_back();
  zmq_msg_init_data(z_propertybag, oarc.buf, oarc.off, 
                    libfault::zmq_msg_standard_free, NULL);

  // third block is the serialization body
  zmq_msg_t* z_body= msg.insert_back();

  if (body != NULL) {
    zmq_msg_init_data(z_body, body, bodylen, libfault::zmq_msg_standard_free, NULL);
  }
  
  // we are giving away the body pointer
  body = NULL;
  clear();
}


EXPORT std::string reply_status_to_string(reply_status status) {
  switch(status) {
   case reply_status::OK:
     return "OK";
   case reply_status::BAD_MESSAGE:
     return "Bad message";
   case reply_status::NO_OBJECT:
     return "No such object ID";
   case reply_status::NO_FUNCTION:
     return "No such function";
   case reply_status::COMM_FAILURE:
     return "Communication Failure";
   case reply_status::AUTH_FAILURE:
     return "Authorization Failure";
   case reply_status::EXCEPTION:
     return "Runtime Exception";
   case reply_status::IO_ERROR:
     return "IO Exception";
   case reply_status::TYPE_ERROR:
     return "Type Exception";
   case reply_status::MEMORY_ERROR:
     return "Memory Exception";
   case reply_status::INDEX_ERROR:
     return "Index Exception";
   default:
     return "";
  }
}

} // cppipc
