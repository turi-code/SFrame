/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <string>
#include <zookeeper_util/key_value.hpp>
namespace libfault {

extern std::string get_zk_objectkey_name(std::string objectkey, size_t nrep);
extern std::string get_publish_key(std::string objectkey);

extern bool master_election(graphlab::zookeeper_util::key_value* zk_keyval,
                     std::string objectkey);

extern bool replica_election(graphlab::zookeeper_util::key_value* zk_keyval,
                     std::string objectkey,
                     size_t replicaid);

} // namespace
