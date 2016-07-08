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
/*  
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


#include <stdio.h>
#include <string.h>
#include <cassert>
#include <map>
#include <graphlab/ui/mongoose/mongoose.h>
#include <sstream>

#include <graphlab.hpp>

#include <graphlab/macros_def.hpp>

static void *callback(enum mg_event event,
                      struct mg_connection *conn,
                      const struct mg_request_info *request_info) {
  if (event == MG_NEW_REQUEST) {
    assert(request_info != NULL);
    
    const std::string url = (request_info->uri == NULL)?
      std::string("/") : std::string(request_info->uri) ;
    const std::string query = (request_info->query_string == NULL)?
      std::string("") : std::string(request_info->query_string) ;

    std::string response = "<p>URL: (" + url + ")</p> <ul>";

 

    std::map<std::string, std::string> map = graphlab::web_util::parse_query(query);
    typedef std::map<std::string, std::string>::value_type pair_type;
    foreach(pair_type pair, map) 
      response += "<li> " + pair.first + " -- " + pair.second + "</li>";
    response += "</ul>";
    


    mg_printf(conn,
              "HTTP/1.1 200 OK\r\n"
              "Content-Type: text/html\r\n"
              "Content-Length: %d\r\n"        // Always set Content-Length
              "\r\n"
              "%s",
              int(response.size()), response.c_str());
 
    // Mark as processed
    return (void*)(1);
  } else {
    return NULL;
  }
}

int main(void) {
  struct mg_context *ctx;
  const char *options[] = {"listening_ports", "8080", NULL};

  ctx = mg_start(&callback, NULL, options);
  getchar();  // Wait until user hits "enter"
  mg_stop(ctx);

  return 0;
}










//// Using lib event

// #include <sys/types.h>
// #include <sys/time.h>
// #include <sys/queue.h>
// #include <stdlib.h>

// #include <err.h>
// #include <event.h>
// #include <evhttp.h>

// void generic_handler(struct evhttp_request *req, void *arg)
// {
//   struct evbuffer *buf;
//   buf = evbuffer_new();
  
//   if (buf == NULL)
//     err(1, "failed to create response buffer");
//   for(size_t i = 0; i < 1000; ++i) {
//     evbuffer_add_printf(buf, "Requested: %s\n", evhttp_request_uri(req));
//   }
//   evhttp_send_reply(req, HTTP_OK, "OK", buf);
// }

// int main(int argc, char **argv)
// {
//     struct evhttp *httpd;

//     event_init();
//     httpd = evhttp_start("0.0.0.0", 8080);

//     /* Set a callback for requests to "/specific". */
//     /* evhttp_set_cb(httpd, "/specific", another_handler, NULL); */

//     /* Set a callback for all other requests. */
//     evhttp_set_gencb(httpd, generic_handler, NULL);

//     event_dispatch();

//     /* Not reached in this code as it is now. */
//     evhttp_free(httpd);

//     return 0;
// }
