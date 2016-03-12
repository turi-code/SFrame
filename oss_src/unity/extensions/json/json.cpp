#include "json_flexible_type.hpp"

#include <unity/lib/toolkit_function_macros.hpp>
#include <unity/lib/toolkit_class_macros.hpp>

#include <iostream>

using namespace graphlab;

static flexible_type _test_flexible_type(const flexible_type& value) {
  std::cout << "flexible_type type is ";
  std::cout << flex_type_enum_to_name(value.get_type()) << std::endl;
  std::cout << "flexible_type value is ";
  std::cout << value;
  return value;
}

BEGIN_FUNCTION_REGISTRATION;
REGISTER_NAMED_FUNCTION("json.dumps", JSON::dumps, "input");
REGISTER_NAMED_FUNCTION("json.loads", JSON::loads, "input");
REGISTER_NAMED_FUNCTION("json._test_flexible_type", _test_flexible_type, "input");
END_FUNCTION_REGISTRATION;
