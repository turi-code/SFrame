/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#ifndef GRAPHLAB_FILEIO_FILEIO_CONSTANTS_HPP
#define GRAPHLAB_FILEIO_FILEIO_CONSTANTS_HPP
#include <string>

namespace graphlab {
namespace fileio {

/**
 * Returns the system temporary directory
 */
std::string get_system_temp_directory();

/**
 * The protocol prefix cache:// to identify a cached file.
 */
std::string get_cache_prefix();

/**
 * The "directory" (cache://tmp/) which all cached files are located in 
 */
std::string get_temp_cache_prefix();

/**
 * The physical directory (/var/tmp) which all cached files are located in .
 * colon seperated.
 */
std::string get_cache_file_locations();
void set_cache_file_locations(std::string);

/**
 * Additional HDFS location for storing large temp files.
 */
std::string get_cache_file_hdfs_location();

/**
 * The initial memory capacity assigned to caches
 */
extern const size_t FILEIO_INITIAL_CAPACITY_PER_FILE;

/**
 * The maximum memory capacity assigned to a cached file until it has to 
 * be flushed.
 */
extern size_t FILEIO_MAXIMUM_CACHE_CAPACITY_PER_FILE;

/**
 * The maximum memory capacity used by all cached files.
 * be flushed.
 */
extern size_t FILEIO_MAXIMUM_CACHE_CAPACITY;

/**
 * The default fileio reader buffer size
 */
extern size_t FILEIO_READER_BUFFER_SIZE; 

/**
 * The default fileio writer buffer size
 */
extern size_t FILEIO_WRITER_BUFFER_SIZE;

/**
 * The alternative ssl certificate file and directory.
 */
const std::string& get_alternative_ssl_cert_dir();
const std::string& get_alternative_ssl_cert_file();
const bool insecure_ssl_cert_checks();

}
}
#endif
