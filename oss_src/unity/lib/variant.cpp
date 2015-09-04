/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <unity/lib/variant.hpp>
#include <flexible_type/flexible_type.hpp>
#include <unity/lib/api/model_interface.hpp>
#include <unity/lib/api/unity_sframe_interface.hpp>
#include <unity/lib/api/unity_sarray_interface.hpp>
#include <unity/lib/api/unity_graph_interface.hpp>
#include <unity/lib/api/function_closure_info.hpp>
namespace graphlab {
namespace archive_detail {

void serialize_impl<oarchive, variant_type, false>::
exec(oarchive& oarc, const variant_type& v) {
  oarc << v.which();
  switch(v.which()) {
   case 0:
     oarc << boost::get<flexible_type>(v);
     break;
   case 1:
     oarc << boost::get<std::shared_ptr<unity_sgraph_base>>(v);
     break;
   case 2:
     oarc << boost::get<dataframe_t>(v);
     break;
   case 3:
     oarc << boost::get<std::shared_ptr<model_base>>(v);
     break;
   case 4:
     oarc << boost::get<std::shared_ptr<unity_sframe_base>>(v);
     break;
   case 5:
     oarc << boost::get<std::shared_ptr<unity_sarray_base>>(v);
     break;
   case 6:
     oarc << boost::get<variant_map_type>(v);
     break;
   case 7:
     oarc << boost::get<variant_vector_type>(v);
     break;
   case 8:
     oarc << boost::get<function_closure_info>(v);
     break;
   default:
     break;
  }
}



void deserialize_impl<iarchive, variant_type, false>::
exec(iarchive& iarc, variant_type& v) {
  int which;
  iarc >> which;
  switch(which) {
   case 0:
     v = flexible_type();
     iarc >> boost::get<flexible_type>(v);
     break;
   case 1:
     v = std::shared_ptr<unity_sgraph_base>();
     iarc >> boost::get<std::shared_ptr<unity_sgraph_base>>(v);
     break;
   case 2:
     v = dataframe_t();
     iarc >> boost::get<dataframe_t>(v);
     break;
   case 3:
     v = std::shared_ptr<model_base>();
     iarc >> boost::get<std::shared_ptr<model_base>>(v);
     break;
   case 4:
     v = std::shared_ptr<unity_sframe_base>();
     iarc >> boost::get<std::shared_ptr<unity_sframe_base>>(v);
     break;
   case 5:
     v = std::shared_ptr<unity_sarray_base>();
     iarc >> boost::get<std::shared_ptr<unity_sarray_base>>(v);
     break;
   case 6:
     v = variant_map_type();
     iarc >> boost::get<variant_map_type>(v);
     break;
   case 7:
     v = variant_vector_type();
     iarc >> boost::get<variant_vector_type>(v);
     break;
   case 8:
     v = function_closure_info();
     iarc >> boost::get<function_closure_info>(v);
     break;
   default:
     break;
  }
}


} // archive_detail
} // graphlab
