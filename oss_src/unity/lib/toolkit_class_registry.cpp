/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <unity/lib/toolkit_class_registry.hpp>
#include <unity/lib/api/model_interface.hpp>

namespace graphlab {

bool toolkit_class_registry::register_toolkit_class(
    const std::string& class_name,
    std::function<model_base*()> constructor,
    std::map<std::string, flexible_type> description) {
  log_func_entry();
  if (registry.count(class_name)) {
    return false;
  } else {
    registry[class_name] = constructor;
    description["name"] = class_name;
    descriptions[class_name] = description;
    return true;
  }
}

bool toolkit_class_registry::register_toolkit_class(
    std::vector<toolkit_class_specification> classes, std::string prefix) {
  bool success = true;
  if (prefix.length() > 0) {
    for (size_t i = 0;i < classes.size(); ++i) {
      classes[i].name = prefix + "." + classes[i].name;
    }
  }
  for (size_t i = 0;i < classes.size(); ++i) {
    success &= register_toolkit_class(classes[i].name, 
                              classes[i].constructor, 
                              classes[i].description);
  }
  return success;
}

std::shared_ptr<model_base> toolkit_class_registry::get_toolkit_class(
    const std::string& class_name) {
  if (registry.count(class_name)) {
    return std::shared_ptr<model_base>(registry[class_name]());
  } else {
    log_and_throw(std::string("Class " + class_name + " does not exist."));
  }
}

std::map<std::string, flexible_type> 
toolkit_class_registry::get_toolkit_class_description(const std::string& class_name) {
  if (descriptions.count(class_name)) {
    return descriptions[class_name];
  } else {
    log_and_throw(std::string("Class" + class_name + " does not exist."));
  }
}
std::vector<std::string> toolkit_class_registry::available_toolkit_classes() {
  std::vector<std::string> ret;
  for (const auto& kv : registry) {
    ret.push_back(kv.first);
  }
  return ret;
}

} // namespace graphlab
