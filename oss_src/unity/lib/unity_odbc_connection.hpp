/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_UNITY_ODBC_CONNECTION_HPP
#define GRAPHLAB_UNITY_ODBC_CONNECTION_HPP

#include <unity/lib/toolkit_class_macros.hpp>
#include <sframe/odbc_connector.hpp>
#include <unity/lib/unity_sframe.hpp>

namespace graphlab {
namespace odbc_connection {

std::vector<graphlab::toolkit_class_specification> get_toolkit_class_registration();

}
}

#endif // GRAPHLAB_UNITY_ODBC_CONNECTION_HPP
