/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
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

#ifndef GRAPHLAB_METRICS_SERVER_HPP
#define GRAPHLAB_METRICS_SERVER_HPP
#include <string>
#include <map>
#include <utility>
#include <boost/function.hpp>


namespace graphlab {



/** 
    \ingroup httpserver
    The callback type used for add_metric_server_callback()
    See add_metric_server_callback() for details.
  */
typedef boost::function<std::pair<std::string, std::string>
                                (std::map<std::string, std::string>&)> 
        http_redirect_callback_type;




/**

  \ingroup httpserver
  \brief This is used to map a URL on the mtrics server 
             to a processing function.
  
  The processing function must have the prototype
  \code
  std::pair<std::string, std::string> callback(std::map<std::string, std::string>&)
  \endcode

  The processing function takes a map of GET variables to their corresponding
  values, and returns a pair of strings. (content_type, content)
  \li \c content type is the http content_type header. For instance text/plain
     or text/html.
  \li \c content is the actual body

  For instance: The builtin 404 handler looks like this:

  \code
  std::pair<std::string, std::string> 
  four_oh_four(std::map<std::string, std::string>& varmap) {
    return std::make_pair(std::string("text/html"), 
                        std::string("Page Not Found"));
  }
  \endcode

  \note The callbacks are only processed on machine 0 since only machine 0
  launches the server.

  \param page The page to map. For instance <code>page = "a.html"</code>
              will be shown on http://[server]/a.html
  \param callback The callback function to use to process the page
 */
void add_metric_server_callback(std::string page, 
                                http_redirect_callback_type callback);

/**
 * \ingroup httpserver
 * Removes a metric server callback.
 * This will remove a callback previously registered with 
 * set_metric_server_callback.
 */
void remove_metric_server_callback(std::string page);

/**
 * \ingroup httpserver
 * \internal
 * Deletes all the metric server callback datastructures.
 * Do not use.
 */
void delete_all_metric_server_callbacks();

/**
  \ingroup httpserver
  \brief Starts the metrics reporting server.

  The function may be called by all machines simultaneously since it only
  does useful work on machine 0. Only machine 0 will launch the web server.

  \param port The port number to use. If port is 0, it will be automatically
              try to use an arbitrary free port.
  
  \returns The port number used. Returns 0 on failure.
 */
size_t launch_metric_server(size_t port = 8090);

/**
  \ingroup httpserver
  \brief Returns the port number the metric server is listening on
  
  \returns The port number used. Returns 0 if metric server is not running.
 */
size_t get_metric_server_port();


/**
  \ingroup httpserver
  \brief Stops the metrics reporting server if one is started.

  The function may be called by all machines simultaneously since it only
  does useful work on machine 0.
 */
void stop_metric_server();

/**
  \ingroup httpserver
  \brief Waits for a ctrl-D on machine 0, and 
         stops the metrics reporting server if one is started.

  The function may be called by all machines simultaneously since it only
  does useful work on machine 0. It waits for the stdin stream to close
  (when the user hit ctrl-d), then shuts down the server.
*/
void stop_metric_server_on_eof();

} // graphlab 
#endif // GRAPHLAB_METRICS_SERVER_HPP
