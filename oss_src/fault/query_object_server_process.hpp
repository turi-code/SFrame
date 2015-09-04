/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef FAULT_QUERY_OBJECT_SERVER_PROCESS_HPP
#define FAULT_QUERY_OBJECT_SERVER_PROCESS_HPP
#include <string>
#include <boost/function.hpp>
#include <fault/query_object.hpp>
#include <fault/query_object_create_flags.hpp>
namespace libfault {
typedef boost::function<query_object*(std::string objectkey,
                                      std::vector<std::string> zk_hosts,
                                      std::string zk_prefix,
                                      uint64_t create_flags)>  query_object_factory_type;

int query_main(int argc, char** argv, const query_object_factory_type& factory);

} // namespace;
#endif

