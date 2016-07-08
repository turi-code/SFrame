/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_GENERIC_AVRO_READER_HPP
#define GRAPHLAB_GENERIC_AVRO_READER_HPP

#include <fstream>

// Get rid of stupid warnings for this library
#ifdef __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-declarations"
#endif


#include <avro/DataFile.hh>
#include <avro/Compiler.hh>
#include <avro/Encoder.hh>
#include <avro/Decoder.hh>
#include <avro/Generic.hh>

#ifdef __GNUC__
#pragma GCC diagnostic pop
#endif

#ifdef __clang__
#pragma clang diagnostic pop
#endif

#include <flexible_type/flexible_type.hpp>

namespace graphlab {

  typedef std::pair<avro::ValidSchema, avro::GenericDatum> schema_datum_pair;
  typedef avro::DataFileReader<schema_datum_pair> avro_reader;

  /**
   * A simple reader to support ingestion of Avro data into SArrays.
   *
   * This reader class leverages the Avro C++ library to decode Avro data files. 
   * To support reading arbitrary data, we leverage the avro::DataFileReader.
   *
   * To do:
   *   - extend DataFileReaderBase::makeStream to use graphlab::general_ifstream
   *   to support reading from S3 and HDFS (in addition to local filesystem)
   *   - update reader to handle directory of Avro files as input
   *   - add support for different codecs (Snappy)
   *   - add support for different reader schemas (that specify a subset of the 
   *   writer schema)
   *   - presently, union types at the root of a schema pose a bit of problem; 
   *   we treat them all as strings, but maybe we do something more 
   *   sophisticated dependending on the combination of types in the union
   *
   * Read more about the Avro specification here: 
   *   http://avro.apache.org/docs/1.7.6/spec.html
   * 
   * And the C++ library here: http://avro.apache.org/docs/1.6.3/api/cpp/html/
   */
  class generic_avro_reader {
    std::unique_ptr<avro::DataFileReader<schema_datum_pair>> reader;
    avro::ValidSchema schema;
    avro::Type schema_type;
    avro::EncoderPtr encoder;
    std::stringstream buffer;
    std::unique_ptr<avro::OutputStream> output;
  
  public:
    /**
     * Default constructor
     *
     * Creates a reader from the Avro data file specified as a string.
     */
    generic_avro_reader(std::string& filename): 
      reader(
        std::unique_ptr<avro_reader>(
          new avro::DataFileReader<schema_datum_pair>(filename.c_str()))),
      schema(reader->readerSchema()),
      schema_type(schema.root()->type()),
      encoder(avro::jsonEncoder(schema)),
      output(avro::ostreamOutputStream(buffer)) {
      encoder->init(*output);

      if (schema.root()->type() == avro::AVRO_NULL)
        log_and_throw(std::string("NULL Avro schema"));

      schema.toJson(buffer);
      logstream(LOG_INFO) << "Initialized Avro reader with schema " 
                          << buffer.str() << std::endl;
      buffer.str("");
    }

    /**
     * Returns base flex_type_enum corresponding to schema type
     *
     * The exact type of the flexible_type can be inferred based on the type 
     * defined in the schema. This method will get the type from the schema 
     * and do the Avro->flexible_type conversion. Returns flex_type_enum.
     */
    flex_type_enum get_flex_type();

    /**
     * Reads a single Avro record as a JSON string
     *
     * Returns a pair where the first item indicates whether there is more to
     * be read, and the next element is the JSON representation of the record.
     */
    std::pair<bool, std::string> read_one_json();
  
    /**
     * Reads a single Avro record as a flexible_type
     *
     * Returns a pair where the first element indicates whether there is more 
     * to be read, and the second is the record converted to a flexible_type.
     */
    std::pair<bool, flexible_type> read_one_flexible_type();
  private:
    flexible_type datum_to_flexible_type(const avro::GenericDatum& datum);
  };

}

#endif
