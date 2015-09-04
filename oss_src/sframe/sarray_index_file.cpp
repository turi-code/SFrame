/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#define BOOST_SPIRIT_THREADSAFE

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/ini_parser.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/filesystem.hpp>
#include <boost/algorithm/string.hpp>
#include <logger/logger.hpp>
#include <logger/assertions.hpp>
#include <sframe/sarray_index_file.hpp>
#include <fileio/general_fstream.hpp>
#include <fileio/fs_utils.hpp>
#include <ini/boost_property_tree_utils.hpp>
#include <serialization/serialization_includes.hpp>
#include <json/json_include.hpp>
namespace graphlab {

void index_file_information::save(oarchive& oarc) const {
  oarc << index_file << version << nsegments << block_size 
       << content_type << segment_sizes << segment_files << metadata;
}
void index_file_information::load(iarchive& iarc) {
  iarc >> index_file >> version >> nsegments >> block_size 
       >> content_type >> segment_sizes >> segment_files >> metadata;
}
index_file_information read_v1_index_file(std::string index_file) {
  index_file_information ret;
  ret.index_file = index_file;

  // try to open the file
  general_ifstream fin(index_file);
  if (fin.fail()) {
    log_and_throw(std::string("Unable to open sarray index file at ") + index_file);
  }

  // parse the file
  boost::property_tree::ptree data;
  try {
    boost::property_tree::ini_parser::read_ini(fin, data);
  } catch(boost::property_tree::ini_parser_error e) {
    log_and_throw(std::string("Unable to parse sarray index file ") + index_file);
  }

  //read the sarray properties.
  try {
    ret.version = std::atoi(data.get<std::string>("sarray.version").c_str());
    if (ret.version != 1) {
      log_and_throw(std::string("Invalid version number. got ")
                    + std::to_string(ret.version));
    }
    ret.nsegments = std::atol(data.get<std::string>("sarray.num_segments").c_str());
    ret.content_type = data.get<std::string>("sarray.content_type", "");

    if (ret.version == 1) {
      ret.block_size = std::atol(data.get<std::string>("sarray.block_size").c_str());
    }
    // now get the segment sizes
    ret.segment_sizes =
        ini::read_sequence_section<size_t>(data, "segment_sizes", ret.nsegments);

    ret.segment_files =
        ini::read_sequence_section<std::string>(data, "segment_files", ret.nsegments);

  } catch(std::string e) {
    log_and_throw(e);
  } catch(...) {
    log_and_throw(std::string("Unable to parse sarray index file ") + index_file);
  }

  // Read the metadata
  if (data.count("metadata")) {
    ret.metadata = ini::read_dictionary_section<std::string>(data, "metadata");
  }

  if (ret.segment_sizes.size() != ret.nsegments ||
      ret.segment_files.size() != ret.nsegments) {
    log_and_throw(std::string("Malformed index_file_information. nsegments mismatch"));
  }


  // if column_files are relative, we need to do fix it up with the index path
  boost::filesystem::path target_index_path(index_file);
  std::string root_dir = target_index_path.parent_path().string();
  for (auto& fname: ret.segment_files) {
    // if it "looks" like a URL continue
    if (fname.empty() || boost::algorithm::contains(fname, "://")) continue;
    // otherwise, it is a local file path
    boost::filesystem::path p(fname);
    // if it is a relative path, we need to fixup with the parent path
    if (p.is_relative()) {
      fname = fileio::make_absolute_path(root_dir, fname);
    }
  }
  return ret;
}

index_file_information read_index_file(std::string index_file) {
  std::pair<std::string, size_t> parsed_fname = parse_v2_segment_filename(index_file);
  // this will read both v1 and v2
  group_index_file_information group_index = read_array_group_index_file(parsed_fname.first);
  logstream(LOG_INFO)  << "Reading index file: " << parsed_fname.first
                       << " column " << parsed_fname.second << std::endl;
  if (parsed_fname.second != (size_t)(-1)) {
    // this must be a v2. I am asking for a specific column
    if (parsed_fname.second < group_index.columns.size()) {
      return group_index.columns[parsed_fname.second];
    } else {
      log_and_throw(std::string("column does not exist in sarray index file at ") + index_file);
    }
  } else {
    // return 0th column
    return group_index.columns[0];
  }
}


void write_index_file(std::string index_file, const index_file_information& info) {
  using boost::filesystem::path;
  using boost::algorithm::starts_with;

  path target_index_path(index_file);
  std::string root_dir = target_index_path.parent_path().string();


  if (info.segment_sizes.size() != info.nsegments ||
      info.segment_files.size() != info.nsegments) {
    log_and_throw(std::string("Malformed index_file_information. nsegments mismatch"));
  }
  // commit the index file
  boost::property_tree::ptree data;
  data.put("sarray.version", info.version);
  data.put("sarray.num_segments", info.nsegments);
  data.put("sarray.content_type", info.content_type);
  if (info.version == 1) {
    data.put("sarray.block_size", info.block_size);
  }

  ini::write_dictionary_section(data, "metadata", info.metadata);
  ini::write_sequence_section(data, "segment_sizes", info.segment_sizes);


  std::vector<std::string> relativized_file_names;
  for (auto filename: info.segment_files) {
    filename = fileio::make_relative_path(root_dir, filename);
    relativized_file_names.push_back(filename);
  }
  ini::write_sequence_section(data, "segment_files", relativized_file_names);

  // now write the index
  general_ofstream fout(index_file);
  boost::property_tree::ini_parser::write_ini(fout, data);
  if (!fout.good()) {
    log_and_throw_io_failure("Fail to write. Disk may be full.");
  }
  fout.close();
}


group_index_file_information read_array_group_index_file(std::string group_index_file) {
  group_index_file_information ret;
  ret.group_index_file = group_index_file;

  // try to open the file
  general_ifstream fin(group_index_file);
  if (fin.fail()) {
    log_and_throw(std::string("Unable to open sarray index file at ") + group_index_file);
  }

  // parse the file
  boost::property_tree::ptree data;
  bool parse_success = false;
  try {
    // try to parse as json
    boost::property_tree::json_parser::read_json(fin, data);
    parse_success = true;
  } catch(...) { }

  if (!parse_success) {
    try {
      // unsucessful. try to parse as ini
      general_ifstream fin(group_index_file);
      boost::property_tree::ini_parser::read_ini(fin, data);
      parse_success = true;
    } catch(...) { }
  }

  if (!parse_success) {
    log_and_throw(std::string("Unable to parse sarray index file ") + group_index_file);
  }

  try {
    // the comon stuff are version, num_segments and segment_files
    ret.version = std::atoi(data.get<std::string>("sarray.version").c_str());
    if (ret.version != 1 && ret.version != 2) {
      log_and_throw(std::string("Invalid version number. got ")
                    + std::to_string(ret.version));
    }

    if (ret.version == 1) {
      // be nice and redirect to the version 1 reader
      ret.columns.push_back(read_v1_index_file(group_index_file));
      ret.version = ret.columns[0].version;
      ret.group_index_file = ret.columns[0].index_file;
      ret.nsegments = ret.columns[0].nsegments;
      ret.segment_files = ret.columns[0].segment_files;
      return ret;
    }

    ret.nsegments = std::atol(data.get<std::string>("sarray.num_segments").c_str());

    ret.segment_files =
        ini::read_sequence_section<std::string>(data, "segment_files", ret.nsegments);

    if (ret.segment_files.size() != ret.nsegments) {
      log_and_throw(std::string("Malformed index_file_information. nsegments mismatch"));
    }

    // if column_files are relative, we need to do fix it up with the index path
    boost::filesystem::path target_index_path(group_index_file);
    std::string root_dir = target_index_path.parent_path().string();
    for (auto& fname: ret.segment_files) {
      // if it "looks" like a URL continue
      if (fname.empty() || boost::algorithm::contains(fname, "://")) continue;
      // otherwise, it is a local file path
      boost::filesystem::path p(fname);
      // if it is a relative path, we need to fixup with the parent path
      if (p.is_relative()) {
        fname = fileio::make_absolute_path(root_dir, fname);
      }
    }
    boost::property_tree::ptree columns = data.get_child("columns");
    size_t column_number = 0;
    for (auto& key_child : columns) {
      auto& child = key_child.second;
      index_file_information info;
      info.version = ret.version;
      info.nsegments = ret.nsegments;
      info.segment_files = ret.segment_files;
      for (std::string& segfile : info.segment_files) {
        if (parse_v2_segment_filename(segfile).second == (size_t)(-1)) {
          segfile = segfile + ":" + std::to_string(column_number);
        }
      }
      info.index_file = group_index_file + ":" + std::to_string(column_number);
      // now get the segment sizes
      info.content_type = child.get<std::string>("content_type", "");
      info.segment_sizes =
          ini::read_sequence_section<size_t>(child, "segment_sizes", info.nsegments);
      if (child.count("metadata")) {
        info.metadata = ini::read_dictionary_section<std::string>(child, "metadata");
      }
      if (info.segment_sizes.size() != info.nsegments) {
        log_and_throw(std::string("Malformed index_file_information. nsegments mismatch"));
      }
      ret.columns.push_back(info);
      ++column_number;
    }

  } catch(std::string e) {
    log_and_throw(e);
  } catch(...) {
    log_and_throw(std::string("Unable to parse sarray index file ") + group_index_file);
  }

  return ret;
}


void write_array_group_index_file(std::string group_index_file,
                                  const group_index_file_information& info) {
  if (info.version == 1) {
    ASSERT_EQ(info.columns.size(), 1);
    write_index_file(group_index_file, info.columns[0]);
    return;
  }

  ASSERT_EQ(info.version, 2);
  using boost::filesystem::path;
  using boost::algorithm::starts_with;

  path target_index_path(group_index_file);
  std::string root_dir = target_index_path.parent_path().string();

  JSONNode data;
  // the comon stuff are version, num_segments and segment_files
  JSONNode sarray_node(JSON_NODE);
  sarray_node.set_name("sarray");
  sarray_node.push_back(JSONNode("version", info.version));
          // the unsigned long needed to avoid issues on some compilers
          // where size_t appears to be ambiguous
  sarray_node.push_back(JSONNode("num_segments", (unsigned long)info.nsegments));
  data.push_back(sarray_node);
  ASSERT_EQ(info.segment_files.size(), info.nsegments);
  // relativize segment files
  std::vector<std::string> relativized_file_names;
  for (auto filename: info.segment_files) {
    filename = fileio::make_relative_path(root_dir, filename);
    relativized_file_names.push_back(filename);
  }
  data.push_back(json::to_json_node("segment_files", relativized_file_names));

  JSONNode columns(JSON_ARRAY);
  columns.set_name("columns");
  for (size_t i = 0;i < info.columns.size(); ++i) {
    JSONNode column(JSON_NODE);
    column.push_back(JSONNode("content_type", info.columns[i].content_type));
    column.push_back(json::to_json_node("metadata", info.columns[i].metadata));
    ASSERT_EQ(info.columns[i].segment_sizes.size(), info.nsegments);
    column.push_back(json::to_json_node("segment_sizes", info.columns[i].segment_sizes));
    columns.push_back(column);
  }
  data.push_back(columns);
  // now write the index
  general_ofstream fout(group_index_file);
  fout << data.write_formatted();

  if (!fout.good()) {
    log_and_throw_io_failure("Fail to write. Disk may be full.");
  }
  fout.close();
}

std::pair<std::string, size_t> parse_v2_segment_filename(std::string fname) {
  boost::algorithm::trim(fname);
  size_t sep = fname.find_last_of(':');
  size_t column_id = (size_t)(-1);
  if (sep != std::string::npos) {
    // there is a : seperator
    char* intendpos = 0;
    std::string trailing_str = fname.substr(sep + 1);
    errno = 0;
    size_t parsed_column_id = std::strtol(trailing_str.c_str(), &intendpos, 10);
    if (errno != ERANGE) {
      if (intendpos == trailing_str.c_str() + trailing_str.length()) {
        // conversion success. We read all the way to the end of the line.
        column_id = parsed_column_id;
        // rewrite the fname string to be just the root index file
        fname = fname.substr(0, sep);
      } // all other cases fail. this is not of the format [file]:N

    } else {
      //not convertble. this is not of the format [file]:N
    }
  }

  return {fname, column_id};
}

} // namespace graphlab
