/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_UNITY_JSON_UTIL_HPP
#define GRAPHLAB_UNITY_JSON_UTIL_HPP
#include <json/json_include.hpp>
#include <unity/lib/api/client_base_types.hpp>
#include <cmath>
namespace graphlab {
  /**
   * Helper utility for converting from flexible_type to json.
   * TODO: Fill in details 
   */
  inline JSONNode flexible_type_to_json(const flexible_type& val, std::string name) {

    if (val.get_type() == flex_type_enum::INTEGER) {
      // long cast needed to avoid a ambiguity error which seems to only show up
      // on mac clang++
      if (std::isnan(val.get<flex_int>())) {
        // treat nan as missing value null
        JSONNode v(JSON_NULL);
        v.set_name(name);
        v.nullify();
        return v;
      } else {
        return JSONNode(name, (double)val.get<flex_int>());
      }
    } else if (val.get_type() == flex_type_enum::FLOAT) {
      if (std::isnan(val.get<double>())) {
        // treat nan as missing value null
        JSONNode v(JSON_NULL);
        v.set_name(name);
        v.nullify();
        return v;
      } else {
        return JSONNode(name, val.get<double>());
      }
    } else if (val.get_type() == flex_type_enum::STRING) {
      return JSONNode(name, val.get<flex_string>());
    } else if (val.get_type() == flex_type_enum::VECTOR) {
      const std::vector<double>& v = val.get<flex_vec>();
      JSONNode vec(JSON_ARRAY);
      for (size_t i = 0;i < v.size(); ++i) {
        JSONNode vecval(JSON_NUMBER);
        vecval= v[i];
        vec.push_back(vecval);
      }
      vec.set_name(name);
      return vec;
    } else if (val.get_type() == flex_type_enum::DICT){
      return JSONNode(name, val.get<flex_string>());
    } else if (val.get_type() == flex_type_enum::UNDEFINED){
      JSONNode v(JSON_NULL);
      v.set_name(name);
      v.nullify();
      return v;
    }

    JSONNode v(JSON_NULL);
    v.set_name(name);
    v.nullify();
    return v;
  }

}
#endif
