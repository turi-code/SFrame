/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <unity/lib/variant.hpp>
#include <unity/lib/api/function_closure_info.hpp>

namespace graphlab {


void function_closure_info::save(oarchive& oarc) const {
  oarc << native_fn_name;
  oarc << arguments.size();
  for (size_t i = 0;i < arguments.size(); ++i) {
    oarc << arguments[i].first << *(arguments[i].second);
  }
}
void function_closure_info::load(iarchive& iarc) {
  arguments.clear();
  size_t nargs = 0;
  iarc >> native_fn_name >> nargs;
  arguments.resize(nargs);
  for (size_t i = 0;i < arguments.size(); ++i) {
    iarc >> arguments[i].first;
    arguments[i].second.reset(new variant_type);
    iarc >> *(arguments[i].second);
  }
}
}
