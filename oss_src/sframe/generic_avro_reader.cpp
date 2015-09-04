/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <fstream>
#include <algorithm>
#include <sframe/generic_avro_reader.hpp>
#include <flexible_type/flexible_type_base_types.hpp>
#include <flexible_type/flexible_type.hpp>
#include <logger/logger.hpp>

namespace graphlab {
  flex_type_enum avro_type_to_flex_type(avro::Type a_type) {
    switch (a_type) {
    case avro::AVRO_STRING:
      return flex_type_enum::STRING;
    case avro::AVRO_BYTES:
      return flex_type_enum::STRING;
    case avro::AVRO_INT:
      return flex_type_enum::INTEGER;
    case avro::AVRO_LONG:
      return flex_type_enum::INTEGER;
    case avro::AVRO_FLOAT:
      return flex_type_enum::FLOAT;
    case avro::AVRO_DOUBLE:
      return flex_type_enum::FLOAT;
    case avro::AVRO_BOOL:
      return flex_type_enum::INTEGER;
    case avro:: AVRO_RECORD:
      return flex_type_enum::DICT;
    case avro::AVRO_ENUM:
      return flex_type_enum::STRING;
    case avro::AVRO_ARRAY:
      return flex_type_enum::LIST;
    case avro::AVRO_MAP:
      return flex_type_enum::DICT;
    case avro::AVRO_FIXED:
      return flex_type_enum::STRING;
    case avro::AVRO_UNION:
      return flex_type_enum::STRING;
    default:
      return flex_type_enum::UNDEFINED;
    }
  }

  flex_type_enum generic_avro_reader::get_flex_type() {
    return avro_type_to_flex_type(schema_type);
  }

  std::pair<bool, std::string> generic_avro_reader::read_one_json() {
    schema_datum_pair p(schema, avro::GenericDatum());
    bool has_more = reader->read(p);
    std::string record;

    if (p.second.type() != avro::AVRO_NULL) {
      avro::encode(*encoder, p.second);
      encoder->flush();
      record = buffer.str();
      buffer.str(std::string());
    }
    
    return std::make_pair(has_more, record);
  }

  typedef std::pair<std::string, avro::GenericDatum> string_datum_pair;
  typedef std::pair<flexible_type, flexible_type> flex_flex_type_pair;

  flex_string bytes_to_flex_str(std::vector<uint8_t> bytes) {
    auto num_bytes = bytes.size();
    std::string output(num_bytes, 0);
    
    for (size_t i = 0; i < num_bytes; i++)
        output[i] = static_cast<char>(bytes[i]);

    return flex_string(output);
  }

  flexible_type generic_avro_reader::datum_to_flexible_type(const avro::GenericDatum& datum) {
    switch (datum.type()) {
    case avro::AVRO_ARRAY: {
      logstream(LOG_DEBUG) << "Parsing AVRO_ARRAY" << std::endl;
      std::vector<avro::GenericDatum> stuff = datum.value<avro::GenericArray>().value();
      flex_list data(stuff.size());

      size_t index = 0;
      for (avro::GenericDatum& d : stuff) {
        data[index] = datum_to_flexible_type(d);
        index++;
      }
      
      return data;
    }
    case avro::AVRO_MAP: {
      logstream(LOG_DEBUG) << "Parsing AVRO_MAP" << std::endl;
      std::vector<string_datum_pair> stuff = datum.value<avro::GenericMap>().value();
      std::vector<flex_flex_type_pair> data(stuff.size());
      
      size_t index = 0;
      for (string_datum_pair& p : stuff) {
        logstream(LOG_DEBUG) << "Adding field " << p.first << std::endl;
        data[index] = { flex_string(p.first), datum_to_flexible_type(p.second) };
        index++;
      }
      
      return flex_dict{data};
    }
    case avro::AVRO_RECORD: {
      logstream(LOG_DEBUG) << "Parsing AVRO_RECORD" << std::endl;
      auto avro_record = datum.value<avro::GenericRecord>();
      size_t num_fields = avro_record.fieldCount();
      std::vector<std::pair<flexible_type, flexible_type>> fields(num_fields);

      for (size_t i = 0; i < num_fields; i++) {
        auto field = avro_record.fieldAt(i);
        auto name = schema.root()->nameAt(i);
        
        logstream(LOG_DEBUG) << "Adding field " << name << std::endl;
        fields[i] = { flex_string(name), datum_to_flexible_type(field) };
      }

      return flex_dict{fields};
    }
    case avro::AVRO_STRING:
      return flex_string(datum.value<std::string>());
    case avro::AVRO_INT:
      return flex_int(datum.value<int32_t>());
    case avro::AVRO_LONG: {
      return flex_int(datum.value<int64_t>());
    }
    case avro::AVRO_FLOAT:
      return flex_float(datum.value<float>());
    case avro::AVRO_DOUBLE:
      return flex_float(datum.value<double>());
    case avro::AVRO_BYTES: {
      logstream(LOG_DEBUG) << "Parsing AVRO_BYTES" << std::endl;
      return bytes_to_flex_str(datum.value<std::vector<uint8_t>>());
    }
    case avro::AVRO_FIXED: {
      logstream(LOG_DEBUG) << "Parsing AVRO_FIXED" << std::endl;
      return bytes_to_flex_str(datum.value<std::vector<uint8_t>>());
    }
    case avro::AVRO_ENUM: {
      logstream(LOG_DEBUG) << "Parsing AVRO_ENUM" << std::endl;
      return flex_string(datum.value<avro::GenericEnum>().symbol());
    }
    case avro::AVRO_UNION: {
      logstream(LOG_DEBUG) << "Parsing AVRO_UNION" << std::endl;
      return datum_to_flexible_type(datum.value<avro::GenericUnion>().value<avro::GenericDatum>());
    }
    case avro::AVRO_BOOL: {
      logstream(LOG_DEBUG) << "Parsing AVRO_BOOL" << std::endl;
      return flex_int((int) datum.value<bool>());
    }
    default:
      return flexible_type();
    }
  }

  std::pair<bool, flexible_type> generic_avro_reader::read_one_flexible_type() {
    schema_datum_pair p(schema, avro::GenericDatum());
    bool has_more = reader->read(p);
    flexible_type record = flex_undefined();

    if (p.second.type() != avro::AVRO_NULL)
      record = datum_to_flexible_type(p.second);
    
    return { has_more, record };
  }
}
