/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_GENERAL_TESTING_UTILS_H_
#define GRAPHLAB_GENERAL_TESTING_UTILS_H_

#include <parallel/pthread_tools.hpp>
#include <util/cityhash_gl.hpp>
#include <util/try_finally.hpp> 
#include <random/random.hpp> 
#include <serialization/serialization_includes.hpp>
#include <vector>
#include <string>
#include <locale>

#include <parallel/mutex.hpp>
#include <sys/types.h>
#include <unistd.h>
#include <boost/filesystem.hpp>

namespace graphlab {

/** The directories we use for our temporary archives should be unique
 *  and everything, but we don't want hundreds of these lying around.
 *  Thus add them to a list with which we delete when the program
 *  exits; this function does that. 
 */
void _add_directory_to_deleter(const std::string& name);

/** Make a unique directory name. 
 */
std::string _get_unique_directory(const std::string& file, size_t line); 


/** Serializes and deserializes a model, making sure that the
 *  model leaves the stream iterator in the appropriate place.
 *
 */
template <typename T, typename U>
void _save_and_load_object(T& dest, const U& src, std::string dir) {

  // Create the directory
  boost::filesystem::create_directory(dir);
  _add_directory_to_deleter(dir); 
  
  std::string arc_name = dir + "/test_archive";

  uint64_t random_number = hash64(random::fast_uniform<size_t>(0,size_t(-1)));

  // Save it
  dir_archive archive_write;
  archive_write.open_directory_for_write(arc_name);

  graphlab::oarchive oarc(archive_write);

  oarc << src << random_number;

  archive_write.close();
  
  // Load it
  dir_archive archive_read;
  archive_read.open_directory_for_read(arc_name);

  graphlab::iarchive iarc(archive_read);

  iarc >> dest;
  
  uint64_t test_number;

  iarc >> test_number;

  archive_read.close();

  ASSERT_EQ(test_number, random_number);
}

#define save_and_load_object(dest, src)                         \
  do{                                                           \
    _save_and_load_object(                                      \
        dest, src, _get_unique_directory(__FILE__, __LINE__));  \
  } while(false)

}

#endif /* _TESTING_UTILS_H_ */
