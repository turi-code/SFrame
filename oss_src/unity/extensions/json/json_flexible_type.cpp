#include "json_flexible_type.hpp"

#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <rapidjson/ostreamwrapper.h>
#include <rapidjson/istreamwrapper.h>
#include <rapidjson/error/en.h>

#include <cassert>
#include <cmath>
#include <stdexcept>

using namespace graphlab;

// TODO znation -- use JSON schema as described in http://rapidjson.org/md_doc_schema.html

typedef rapidjson::Writer<rapidjson::OStreamWrapper> json_writer;

// forward declarations
static void _dump(flexible_type input, json_writer& output);
static flexible_type _load(const rapidjson::Value& root);
static flexible_type _extract(const flexible_type& value);

flex_string JSON::dumps(flexible_type input) {
  std::stringstream output;
  JSON::dump(input, output);
  return output.str();
}

static void _dump_int(flex_int input, json_writer& output) {
  // we can write integers of any size -- they turn into 64-bit float in JS,
  // but the serialization format doesn't specify a max int value.
  // see http://stackoverflow.com/questions/13502398/json-integers-limit-on-size
  output.Int64(input);
}

static void _dump_float(flex_float input, json_writer& output) {
  // Float values (like 0.0 or -234.56) are allowed in JSON,
  // but not inf, -inf, or nan. Have to adjust for those.
  if (!std::isnan(input) && !std::isinf(input)) {
    // finite float value, can output as is.
    output.Double(input);
    return;
  }

  output.StartObject();
  output.Key("type");
  output.String(flex_type_enum_to_name(flex_type_enum::FLOAT));
  output.Key("value");
  if (std::isnan(input)) {
    output.String("NaN");
  } else {
    assert(std::isinf(input));
    if (input > 0) {
      output.String("Infinity");
    } else {
      output.String("-Infinity");
    }
  }
  output.EndObject();
}

static void _dump_string(flex_string input, json_writer& output) {
  output.String(input.c_str(), input.size());
}

template<typename T>
static void _dump(std::vector<T> input, json_writer& output, flex_type_enum type_hint) {
  output.StartObject();
  output.Key("type");
  output.String(flex_type_enum_to_name(type_hint));
  output.Key("value");
  output.StartArray();
  for (const T& element : input) {
    _dump(element, output);
  }
  output.EndArray();
  output.EndObject();
}

static void _dump_vector(flex_vec input, json_writer& output) {
  _dump<flex_float>(input, output, flex_type_enum::VECTOR);
}

static void _dump_list(flex_list input, json_writer& output) {
  _dump<flexible_type>(input, output, flex_type_enum::LIST);
}

static void _dump_dict(flex_dict input, json_writer& output) {
  output.StartObject();
  output.Key("type");
  output.String(flex_type_enum_to_name(flex_type_enum::DICT));
  output.Key("value");
  output.StartObject();
  for (const auto& kv : input) {
    const flex_string& key = kv.first;
    const flexible_type& value = kv.second;
    output.Key(key.c_str());
    _dump(value, output);
  }
  output.EndObject();
  output.EndObject();
}

static void _dump_date_time(flex_date_time input, json_writer& output) {
  int32_t time_zone_offset = input.time_zone_offset();
  _dump<flexible_type>(std::vector<flexible_type>({
    input.posix_timestamp(),
    time_zone_offset == 64 ? FLEX_UNDEFINED : flexible_type(flex_int(time_zone_offset)),
    input.microsecond()
  }), output, flex_type_enum::DATETIME);
}

static void _dump_image(flex_image input, json_writer& output) {
  output.StartObject();
  output.Key("type");
  output.String(flex_type_enum_to_name(flex_type_enum::IMAGE));
  output.Key("value");
  output.StartObject();
  output.Key("height");
  _dump_int(input.m_height, output);
  output.Key("width");
  _dump_int(input.m_width, output);
  output.Key("channels");
  _dump_int(input.m_channels, output);
  output.Key("size");
  _dump_int(input.m_image_data_size, output);
  output.Key("version");
  _dump_int(input.m_version, output);
  output.Key("format");
  _dump_int(static_cast<flex_int>(input.m_format), output);
  output.Key("image_data");
  _dump_string(flex_string(
    reinterpret_cast<const char *>(input.get_image_data()),
    input.m_image_data_size
  ), output);
  output.EndObject();
  output.EndObject();
}

static void _dump(flexible_type input, json_writer& output) {
  switch (input.get_type()) {
    case flex_type_enum::INTEGER:
      _dump_int(input.get<flex_int>(), output);
      break;
    case flex_type_enum::FLOAT:
      _dump_float(input.get<flex_float>(), output);
      break;
    case flex_type_enum::STRING:
      _dump_string(input.get<flex_string>(), output);
      break;
    case flex_type_enum::VECTOR:
      _dump_vector(input.get<flex_vec>(), output);
      break;
    case flex_type_enum::LIST:
      _dump_list(input.get<flex_list>(), output);
      break;
    case flex_type_enum::DICT:
      _dump_dict(input.get<flex_dict>(), output);
      break;
    case flex_type_enum::DATETIME:
      _dump_date_time(input.get<flex_date_time>(), output);
      break;
    case flex_type_enum::IMAGE:
      _dump_image(input.get<flex_image>(), output);
      break;
    case flex_type_enum::UNDEFINED:
      output.Null();
      break;
  }
}

void JSON::dump(flexible_type input, std::ostream& output) {
  rapidjson::OStreamWrapper wrapper(output);
  json_writer writer(wrapper);
  _dump(input, writer);
}

static flex_list _load_array(const rapidjson::Value& array) {
  flex_list ret;
  for (rapidjson::Value::ConstValueIterator itr = array.Begin();
       itr != array.End();
       ++itr) {
    ret.push_back(_load(*itr));
  }
  return ret;
}

static flex_string _load_string(const rapidjson::Value& str) {
  return flex_string(str.GetString(), str.GetStringLength());
}

static flex_dict _load_object(const rapidjson::Value& object) {
  flex_dict ret;
  for (rapidjson::Value::ConstMemberIterator itr = object.MemberBegin();
       itr != object.MemberEnd();
       ++itr) {
    ret.push_back(std::make_pair<flex_string, flexible_type>(
      _load_string(itr->name),
      _load(itr->value)));
  }
  return ret;
}

static flexible_type _load_number(const rapidjson::Value& number) {
  if (number.IsFloat() || number.IsDouble()) {
    return flex_float(number.GetDouble());
  }
  return flex_int(number.GetInt64());
}

static flexible_type _load(const rapidjson::Value& root) {
  switch (root.GetType()) {
    case rapidjson::kNullType:
      return FLEX_UNDEFINED;
    case rapidjson::kFalseType:
    case rapidjson::kTrueType:
      throw "Boolean values are not handled by flexible_type.";
    case rapidjson::kObjectType:
      return _load_object(root);
    case rapidjson::kArrayType:
      return _load_array(root);
    case rapidjson::kStringType:
      return _load_string(root);
    case rapidjson::kNumberType:
      return _load_number(root);
  }
}

static flex_float _extract_float(const flexible_type& value) {
  flex_string str_value = value.get<flex_string>();
  if (str_value == "NaN") {
    return flex_float(NAN);
  } else if (str_value == "Infinity") {
    return flex_float(INFINITY);
  } else if (str_value == "-Infinity") {
    return flex_float(-INFINITY);
  } else {
    throw "Unsupported input for tagged float value. Expected one of the strings \"NaN\", \"Infinity\", or \"-Infinity\".";
  }
}

template<typename T>
static T _extract_list(const flexible_type& value) {
  flex_list input = value.get<flex_list>();
  T ret;
  for (const auto& element : input) {
    ret.push_back(_extract(element));
  }
  return ret;
}

static flex_dict _extract_dict(const flexible_type& value) {
  flex_dict input = value.get<flex_dict>();
  flex_dict ret;
  for (const auto& kv : input) {
    ret.push_back(std::make_pair<flex_string, flexible_type>(
      _extract(kv.first),
      _extract(kv.second)
    ));
  }
  return ret;
}

static flex_date_time _extract_date_time(const flexible_type& value) {
  flex_list input = value.get<flex_list>();
  int64_t posix_timestamp = input[0].get<flex_int>();
  int32_t tz_15_min_offset;
  if (input[1].get_type() == flex_type_enum::UNDEFINED) {
    tz_15_min_offset = flex_date_time::EMPTY_TIMEZONE;
  } else {
    tz_15_min_offset = input[1].get<flex_int>();
  }
  int32_t microsecond = input[2].get<flex_int>();
  return flex_date_time(
    posix_timestamp,
    tz_15_min_offset,
    microsecond
  );
}

static flexible_type _dict_get(const flex_dict& dict, const flex_string& key) {
  for (const auto& kv : dict) {
    if (kv.first == key) {
      return kv.second;
    }
  }
  std::stringstream msg;
  msg << "Expected key \"";
  msg << key;
  msg << "\" was not present in dictionary input.";
  throw msg.str();
}

static flex_image _extract_image(const flexible_type& value) {
  flex_string image_data_str = _dict_get(value, "image_data");
  const char *image_data = image_data_str.c_str();
  flex_int height = _dict_get(value, "height");
  flex_int width = _dict_get(value, "width");
  flex_int channels = _dict_get(value, "channels");
  flex_int image_data_size = _dict_get(value, "size");
  flex_int version = _dict_get(value, "version");
  flex_int format = _dict_get(value, "format");
  return flex_image(
    image_data,
    height,
    width,
    channels,
    image_data_size,
    version,
    format
  );
}

static flexible_type _extract(const flexible_type& value) {
  if (value.get_type() != flex_type_enum::DICT) {
    // only dict type encodes type tag
    return value;
  }
  flex_dict value_dict = value.get<flex_dict>();
  if (value_dict.size() != 2) {
    throw "Expected a dictionary with exactly two keys (\"type\" and \"value\").";
  }

  flex_string type_tag_string = _dict_get(value_dict, "type").get<flex_string>();
  flexible_type underlying_value = _dict_get(value_dict, "value");
  flex_type_enum type_tag = flex_type_enum_from_name(type_tag_string);
  switch (type_tag) {
    case flex_type_enum::FLOAT:
      return _extract_float(underlying_value);
    case flex_type_enum::VECTOR:
      return _extract_list<flex_vec>(underlying_value);
    case flex_type_enum::LIST:
      return _extract_list<flex_list>(underlying_value);
    case flex_type_enum::DICT:
      return _extract_dict(underlying_value);
    case flex_type_enum::DATETIME:
      return _extract_date_time(underlying_value);
    case flex_type_enum::IMAGE:
      return _extract_image(underlying_value);
    default:
      throw "Type tag is not a supported type tag for flexible_type JSON serialization. The tagged type probably has a non-lossy underlying representation in plain JSON.";
  }
}

flexible_type JSON::loads(flex_string input) {
  std::stringstream stream(input);
  return JSON::load(stream);
}

flexible_type JSON::load(std::istream& input) {
  rapidjson::IStreamWrapper wrapper(input);
  rapidjson::Document document;
  document.ParseStream(wrapper);
  if (document.HasParseError()) {
    throw rapidjson::GetParseError_En(document.GetParseError());
  }
  flexible_type ret = _load(document); // naive load from JSON
  ret = _extract(ret); // transform to original flexible_type representation
  return ret;
}
