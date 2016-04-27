#include <unity/lib/variant.hpp>

namespace graphlab {
  namespace JSON {
    // variant_type -> variant_type
    // where the input is an arbitrary variant_type,
    // and the output is guaranteed to be naively JSON serializable.
    variant_type to_serializable(variant_type input);
  }
}

