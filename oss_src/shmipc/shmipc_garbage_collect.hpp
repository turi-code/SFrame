/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_SHMIPC_GARBAGE_COLLECT_HPP
#define GRAPHLAB_SHMIPC_GARBAGE_COLLECT_HPP
#include <memory>
namespace graphlab {
namespace shmipc {

// forward decl of raii deleter
struct raii_deleter;

/**
 * For a given shared memory segment name, m_name, this function
 * returns a scoped deleter object which unlinks the shared memory segment
 * on destruction.
 *
 * Internally, the RAII deleter contains a pair of a shared memory filename and
 * a "tag" filename which is used to indicate that the shared memory file
 * exists.  The tag filename is used in shared memory garbage collection and is
 * located as [system temp directory]/glshm_[userid]/[shmname]
 *
 * The reason for having a directory for each userid is to avoid permission
 * issues. Each tag file contains inside of it, the PID of the server process.
 */
std::shared_ptr<raii_deleter> 
register_shared_memory_name(std::string m_name); 

/**
 * Collects all unused shared memory segments
 */
void garbage_collect();
} // shmipc
} // graphlab
#endif
