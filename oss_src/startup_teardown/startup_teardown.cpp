/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <sys/time.h>
#ifndef _WIN32
#include <sys/resource.h>
#endif
#include <globals/globals.hpp>
#include <globals/global_constants.hpp>
#include <sframe/sframe_constants.hpp>
#include <sframe/sframe_config.hpp>
#include <fileio/file_download_cache.hpp>
#include <fileio/fixed_size_cache_manager.hpp>
#include <fileio/temp_files.hpp>
#include <fileio/block_cache.hpp>
#include <parallel/thread_pool.hpp>
#include <logger/logger.hpp>
#include <logger/log_rotate.hpp>
#include <minipsutil/minipsutil.h>
#include "startup_teardown.hpp"
namespace graphlab {


/**
 * Attempts to increase the file handle limit. 
 * Returns true on success, false on failure.
 */
bool upgrade_file_handle_limit(size_t limit) {
#ifndef _WIN32
  struct rlimit rlim;
  rlim.rlim_cur = limit;
  rlim.rlim_max = limit;
  return setrlimit(RLIMIT_NOFILE, &rlim) == 0;
#else
  return true;
#endif
}


/**
 * Gets the current file handle limit.
 * Returns the current file handle limit on success, 
 * -1 on infinity, and 0 on failure.
 */
int get_file_handle_limit() {
#ifndef _WIN32
  struct rlimit rlim;
  int ret = getrlimit(RLIMIT_NOFILE, &rlim);
  if (ret != 0) return 0;
  return int(rlim.rlim_cur);
#else
  return 4096;
#endif
}

void configure_global_environment(std::string argv0) {

  // file limit upgrade has to be the very first thing that happens. The 
  // reason is that on Mac, once a file descriptor has been used (even STDOUT),
  // the file handle limit increase will appear to work, but will in fact fail
  // silently.
  upgrade_file_handle_limit(4096);
  int file_handle_limit = get_file_handle_limit();
  if (file_handle_limit < 4096) {
    logstream(LOG_WARNING) 
        << "Unable to raise the file handle limit to 4096. "
        << "Current file handle limit = " << file_handle_limit << ". "
        << "You may be limited to frames with about " << file_handle_limit / 16 
        << " columns" << std::endl;
  }
  // if file handle limit is >= 512,
  //    we take either 3/4 of the limit
  // otherwise
  //    we keep it to 128
  if (file_handle_limit >= 512) {
    size_t practical_limit = file_handle_limit / 4 * 3;
    graphlab::SFRAME_FILE_HANDLE_POOL_SIZE = practical_limit;
  } else {
    graphlab::SFRAME_FILE_HANDLE_POOL_SIZE = 128;
  }

  graphlab::SFRAME_DEFAULT_NUM_SEGMENTS = graphlab::thread::cpu_count();
  graphlab::SFRAME_MAX_BLOCKS_IN_CACHE = 16 * graphlab::thread::cpu_count();
  graphlab::SFRAME_SORT_MAX_SEGMENTS = 
      std::max(graphlab::SFRAME_SORT_MAX_SEGMENTS, graphlab::SFRAME_FILE_HANDLE_POOL_SIZE / 4);
  // configure all memory constants
  // use up at most half of system memory.
  size_t total_system_memory = total_mem();
  total_system_memory /= 2;
  char* envval = getenv("DISABLE_MEMORY_AUTOTUNE");
  bool disable_memory_autotune = envval != nullptr && (std::string(envval) == "1");

  
  // memory limit
  envval = getenv("GRAPHLAB_MEMORY_LIMIT_IN_MB");
  if (envval != nullptr) {
    size_t limit = atoll(envval) * 1024 * 1024; /* MB */
    if (limit == 0) {
      logstream(LOG_WARNING) << "GRAPHLAB_MEMORY_LIMIT_IN_MB environment "
                                "variable cannot be parsed" << std::endl;
    } else {
      total_system_memory = limit;
    }
  }

  if (total_system_memory > 0 && !disable_memory_autotune) {
    // TODO: MANY MANY HEURISTICS 
    // assume we have 1/2 of working memory to do things like sort, join, etc.
    // and the other 1/2 of working memory goes to file caching
    // HUERISTIC 1: Cell size estimate is 64
    // HUERISTIC 2: Row size estimate is Cell size estimate * 5
    //
    // Also, we only allow upgrades on the existing conservative values when
    // duing these estimates to prevent us from having impractically small 
    // values.
    size_t CELL_SIZE_ESTIMATE = 64;
    size_t ROW_SIZE_ESTIMATE = CELL_SIZE_ESTIMATE * 5;
    size_t max_cell_estimate = total_system_memory / 4 / CELL_SIZE_ESTIMATE;
    size_t max_row_estimate = total_system_memory / 4 / ROW_SIZE_ESTIMATE;

    graphlab::SFRAME_GROUPBY_BUFFER_NUM_ROWS = max_row_estimate;
    graphlab::SFRAME_JOIN_BUFFER_NUM_CELLS = max_cell_estimate;
    graphlab::sframe_config::SFRAME_SORT_BUFFER_SIZE = total_system_memory / 4;
    graphlab::fileio::FILEIO_MAXIMUM_CACHE_CAPACITY_PER_FILE = total_system_memory / 2;
    graphlab::fileio::FILEIO_MAXIMUM_CACHE_CAPACITY = total_system_memory / 2;
  }
  graphlab::globals::initialize_globals_from_environment(argv0);
  
  graphlab::reap_unused_temp_files();

  // force initialize rng
  graphlab::random::get_source();

}


void global_teardown::perform_teardown() {
  if (teardown_performed) return;
  teardown_performed = true;

  try {
    graphlab::fileio::fixed_size_cache_manager::get_instance().clear();
    graphlab::file_download_cache::get_instance().clear();
    graphlab::block_cache::release_instance();
    graphlab::reap_current_process_temp_files();
    graphlab::reap_unused_temp_files();
    graphlab::stop_log_rotation();
    graphlab::thread_pool::release_instance();
    graphlab::timer::stop_approx_timer();
  } catch (...) {
    std::cerr << "Exception on teardown." << std::endl;
  }
}

global_teardown::~global_teardown() {
}

namespace teardown_impl {
// we use an externed global variable so that only one occurance of this 
// object shows up after many shared library linkings.
global_teardown teardown_instance;
} // teardown_impl

global_teardown& global_teardown::get_instance() {
  // we use a
  return teardown_impl::teardown_instance;
}

} // graphlab
