/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
/// \ingroup fault
#ifndef GRAPHLAB_ZOOKEEPER_COMMON_HPP
#define GRAPHLAB_ZOOKEEPER_COMMON_HPP
#include <vector>
#include <string>

extern "C" {
#include <zookeeper/zookeeper.h>
}


namespace graphlab {
namespace zookeeper_util {

/// frees a zookeeper String_vector
void free_String_vector(struct String_vector* strings); 

/// convert a zookeeper String_vector to a c++ vector<string>
std::vector<std::string> String_vector_to_vector(
    const struct String_vector* strings); 

/// print a few zookeeper error status
void print_stat(int stat, 
                const std::string& prefix, 
                const std::string& path); 

/// adds a trailing / to the path name if there is not one already
std::string normalize_path(std::string prefix); 

/// Creates a zookeeper directory
int create_dir(zhandle_t* handle, const std::string& path, 
               const std::string& stat_message = "");

/// Deletes a zookeeper directory
int delete_dir(zhandle_t* handle, const std::string& path,
               const std::string& stat_message = "");

/// Creates a zookeeper ephemeral node
int create_ephemeral_node(zhandle_t* handle, 
                 const std::string& path, 
                 const std::string& value,
                 const std::string& stat_message = "");

/// Deletes a zookeeper ephemeral node 
int delete_node(zhandle_t* handle, 
                const std::string& path,
                const std::string& stat_message = "");

/// Deletes a zookeeper sequence node 
int delete_sequence_node(zhandle_t* handle, 
                         const std::string& path,
                         const int version,
                         const std::string& stat_message = "");

/// Gets the effective node name for a sequence node of a particular sequence number
std::string get_sequence_node_path(const std::string& path,
                                   const int version);


/// Creates a zookeeper ephemeral sequence nodea
/// Returns a pair of (status, version)
std::pair<int, int> create_ephemeral_sequence_node(zhandle_t* handle, 
                                                   const std::string& path, 
                                                   const std::string& value,
                                                   const std::string& stat_message = "");

/// Gets the value in a node. output is a pair of (success, value)
std::pair<bool, std::string> get_node_value(zhandle_t* handle,
                                            const std::string& node,
                                            const std::string& stat_message = "");


} // graphlab
} // zookeeper 

#endif
