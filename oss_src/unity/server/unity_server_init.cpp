/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <boost/filesystem.hpp>
#include <unity/lib/unity_sframe.hpp>
#include <unity/lib/unity_sgraph.hpp>
#include <unity/lib/unity_sarray.hpp>
#include <unity/lib/unity_sketch.hpp>
#include <unity/lib/unity_global.hpp>
#include <unity/lib/unity_global_singleton.hpp>
#include <unity/lib/simple_model.hpp>
#include <unity/lib/toolkit_class_registry.hpp>
#include <unity/lib/toolkit_function_registry.hpp>
#include <unity/lib/unity_odbc_connection.hpp>
#include <unity/server/unity_server_init.hpp>
#include <unity/toolkits/image/image_fn_export.hpp>

namespace graphlab {

void unity_server_initializer::init_toolkits(toolkit_function_registry& registry) const {
  registry.register_toolkit_function(image_util::get_toolkit_function_registration());
}

void unity_server_initializer::init_models(toolkit_class_registry& registry) const {
  register_model_helper<simple_model>(registry);
  registry.register_toolkit_class(odbc_connection::get_toolkit_class_registration(), "_odbc_connection");
}

void unity_server_initializer::init_extensions(std::string root_path_, std::shared_ptr<unity_global> unity_global_ptr) const {
  using namespace fileio;
  namespace fs = boost::filesystem;
  fs::path root_path(root_path_);
  // look for shared libraries I can load
  std::vector<fs::path> candidate_paths {root_path / "*.so", 
                                               root_path / "*.dylib", 
                                               root_path / "*.dll", 
                                               root_path / "../extensions/*.so", 
                                               root_path / "../extensions/*.dylib",
                                               root_path / "../extensions/*.dll"};
  // we exclude all of our own libraries
  std::vector<fs::path> exclude_paths {root_path / "*libunity*.so", 
                                             root_path / "*libunity*.dylib", 
                                             root_path / "*libunity*.dll"};
  std::set<std::string> exclude_files;
  for (auto exclude_candidates: exclude_paths) { 
    auto globres = get_glob_files(exclude_candidates.string());
    for (auto file : globres) exclude_files.insert(file.first);
  }

  for (auto candidates: candidate_paths) { 
    for (auto file : get_glob_files(candidates.string())) {
      // exclude files in the exclusion list
      if (exclude_files.count(file.first)) {
        logstream(LOG_INFO) << "Excluding load of " << file.first << std::endl;
        continue;
      }
      // exclude libhdfs
      if (boost::ends_with(file.first, "libhdfs.so")) continue;
      if (file.second == file_status::REGULAR_FILE) {
        logstream(LOG_INFO) << "Autoloading of " << file.first << std::endl;
        unity_global_ptr->load_toolkit(file.first, "..");
      }
    }
  }
}

void unity_server_initializer::register_base_classes(cppipc::comm_server* server,
                                                     std::shared_ptr<unity_global> unity_global_ptr) const {
  server->register_type<unity_sgraph_base>([](){ 
                                            return new unity_sgraph();
                                          });
  server->register_type<model_base>([](){ 
                                            return new simple_model();
                                          });
  server->register_type<unity_sframe_base>([](){ 
                                            return new unity_sframe();
                                          });
  server->register_type<unity_sarray_base>([](){ 
                                            return new unity_sarray();
                                          });
  server->register_type<unity_sketch_base>([](){ 
                                            return new unity_sketch();
                                          });
  // Require unity_global singleton to be created first
  server->register_type<graphlab::unity_global_base>([=]()->std::shared_ptr<graphlab::unity_global_base> {
    return std::dynamic_pointer_cast<graphlab::unity_global_base>(unity_global_ptr);
  });
};




} // end of namespace graphlab
