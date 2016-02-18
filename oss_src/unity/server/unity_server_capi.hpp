/**
 * Copyright (C) 2016 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_UNITY_SERVER_CAPI_H
#define GRAPHLAB_UNITY_SERVER_CAPI_H

#include <unity/server/unity_server.hpp>
#include <unity/server/unity_server_options.hpp>

namespace graphlab {

void start_server(const unity_server_options& server_options);
void* get_client();
void stop_server();
void set_log_progress_callback( void (*callback)(const std::string&));
void set_log_progress(bool enable);
} // end of graphlab




#endif /* UNITY_SERVER_CAPI_H */
