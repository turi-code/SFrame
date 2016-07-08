/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <util/testing_utils.hpp>

namespace graphlab {

////////////////////////////////////////////////////////////////////////////////

static std::vector<std::string> __list_of_directories_to_clean_up;
static mutex __list_of_directories_lock;
static bool archive_directory_deleter_added = false; 

void _archive_directory_deleter() {
  
  std::lock_guard<mutex> lgaurd(__list_of_directories_lock);

  for(const std::string& dir : __list_of_directories_to_clean_up) {
    try { 
      boost::filesystem::remove_all(dir);
    } catch(...) {
      // Ignore it. 
    }
  }
}

/** The directories we use for our temporary archives should be unique
 *  and everything, but we don't want hundreds of these lying around.
 *  Thus add them to a list with which we delete when the program
 *  exits; this function does that. 
 */
void _add_directory_to_deleter(const std::string& name) {

  __list_of_directories_lock.lock();
  
  if(!archive_directory_deleter_added) {
    std::atexit(_archive_directory_deleter);
    archive_directory_deleter_added = true;
  }
        
  __list_of_directories_to_clean_up.push_back(name);
  
  __list_of_directories_lock.unlock();
}


/** Make a unique directory name. 
 */
std::string _get_unique_directory(const std::string& file, size_t line) {
  
  std::ostringstream ss;

  ss << "./archive_" << getpid() << "_";
  
  ss  << "t" << thread::thread_id() << "__";

  ss << random::fast_uniform<size_t>(0, size_t(-1)); 

  return ss.str();
}
  


}
