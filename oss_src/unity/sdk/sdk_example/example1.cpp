/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#include <string>
#include <vector>
#include <flexible_type/flexible_type.hpp>
#include <graphlab/sdk/toolkit_function_macros.hpp>
#include <graphlab/sdk/toolkit_class_macros.hpp>
#include <graphlab/sdk/gl_sarray.hpp>
#include <graphlab/sdk/gl_sframe.hpp>
#include <graphlab/sdk/gl_sgraph.hpp>
using namespace graphlab;
flexible_type add(flexible_type a, flexible_type b) {
  return a + b;
}

double multiply_by_two(double b) {
  return 2*b;
}


double apply_lambda(std::function<double(double)> f, size_t i) {
  return f(i);
}

std::string replicate(const std::map<std::string, flexible_type>& input) {
  std::string ret;
  size_t reptimes = input.at("reptimes");
  std::string value = input.at("value");
  for (size_t i = 0;i < reptimes; ++i) {
    ret = ret + value;
  }
  return ret;
}

std::vector<variant_type> connected_components(std::map<std::string, flexible_type>& src,
                                               std::map<std::string, flexible_type>& edge,
                                               std::map<std::string, flexible_type>& dst) {
  if (src["cc"] < dst["cc"]) dst["cc"] = src["cc"];
  else src["cc"] = dst["cc"];
  return {to_variant(src), to_variant(edge), to_variant(dst)};
}



class demo_class: public toolkit_class_base {
  virtual void save_impl(oarchive& oarc) const  {}

  virtual void load_version(iarchive& iarc, size_t version) {}

  // Simple Functions
  std::string concat() {
    return one + two;
  }

  // Simple Functions
  std::string concat_more(std::string three) {
    return one + two + three;
  }
  // simple properties
  std::string one;
  std::string two;

  // getter/setter properties
  std::string two_getter() const {
    return two;
  }
  void two_setter(std::string param) {
    two = param + " pika";
  }

  BEGIN_CLASS_MEMBER_REGISTRATION("demo_class")
  REGISTER_CLASS_MEMBER_FUNCTION(demo_class::concat);
  REGISTER_CLASS_MEMBER_FUNCTION(demo_class::concat_more, "three");

  REGISTER_PROPERTY(one);

  REGISTER_GETTER("two", demo_class::two_getter);
  REGISTER_SETTER("two", demo_class::two_setter);
  END_CLASS_MEMBER_REGISTRATION
};



BEGIN_FUNCTION_REGISTRATION
REGISTER_FUNCTION(add, "a", "b");
REGISTER_FUNCTION(multiply_by_two, "b");
REGISTER_FUNCTION(apply_lambda, "lb", "value");
REGISTER_FUNCTION(replicate, "input");
REGISTER_FUNCTION(connected_components, "src", "edge", "dst");
REGISTER_DOCSTRING(add, "adds two numbers")
END_FUNCTION_REGISTRATION



BEGIN_CLASS_REGISTRATION
REGISTER_CLASS(demo_class)
END_CLASS_REGISTRATION
