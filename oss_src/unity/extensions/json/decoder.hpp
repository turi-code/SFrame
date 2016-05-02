#include <unity/lib/variant.hpp>

namespace graphlab {
  namespace JSON {
    // flexible_type, flexible_type -> variant_type
    // where the input is a pair (data, schema)
    // as produced by JSON::to_serializable, and the
    // output is the original underlying variant_type.
    variant_type from_serializable(flexible_type data, variant_type schema);
  }
}
