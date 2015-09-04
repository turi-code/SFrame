/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <logger/logger.hpp>
#include <fileio/union_fstream.hpp>
#include <fileio/cache_stream.hpp>
#include <fileio/s3_fstream.hpp>
#include <fileio/curl_downloader.hpp>
#include <fileio/file_download_cache.hpp>
#include <fileio/sanitize_url.hpp>
#include <boost/algorithm/string.hpp>
#include <fileio/fs_utils.hpp>
#include <fileio/hdfs.hpp>
namespace graphlab {
/**
 * A simple union of std::fstream and graphlab::hdfs::fstream, and graphlab::fileio::cache_stream.
 */
union_fstream::union_fstream(std::string url,
                             std::ios_base::openmode mode,
                             std::string proxy) : url(url) {
  input_stream = NULL;
  output_stream = NULL;

  if ((mode & std::ios_base::in) && (mode & std::ios_base::out)) {
    // If the mode is both in and out, raise exception.
    log_and_throw_io_failure("Invalid union_fstream open mode: cannot be both in and out");
  }
  else if (!(mode & std::ios_base::in) && !(mode & std::ios_base::out)) {
    // If the mode is neither in nor out, raise exception.
    log_and_throw_io_failure("Invalid union_fstream open mode: cannot be neither in nor out");
  }

  bool is_output_stream = (mode & std::ios_base::out);

  if(boost::starts_with(url, "hdfs://")) {
    // HDFS file type
    type = HDFS;
    std::string host, port, path;
    std::tie(host, port, path) = fileio::parse_hdfs_url(url);
    logstream(LOG_INFO) << "HDFS URL parsed: Host: " << host << " Port: " << port
                        << " Path: " << path << std::endl;
    if (host.empty() && port.empty() && path.empty()) {
      log_and_throw_io_failure("Invalid hdfs url: " + url);
    }
    try {
      auto& hdfs = graphlab::hdfs::get_hdfs(host, std::stoi(port));
      ASSERT_TRUE(hdfs.good());
      if (is_output_stream) {
        output_stream.reset(new graphlab::hdfs::fstream(hdfs, path, true));
      } else {
        input_stream.reset(new graphlab::hdfs::fstream(hdfs, path, false));
        m_file_size = hdfs.file_size(path);
      }
    } catch(...) {
      log_and_throw_io_failure("Unable to open " + url);
    }
  } else if (boost::starts_with(url, fileio::get_cache_prefix())) {
    // Cache file type
    type = CACHE;
    if (is_output_stream) {
      output_stream.reset(new fileio::ocache_stream(url));
    } else {
      auto cachestream = std::make_shared<fileio::icache_stream>(url);
      input_stream = (*cachestream)->get_underlying_stream();
      if (input_stream == nullptr) input_stream = cachestream;
      m_file_size = (*cachestream)->file_size();
    }
  } else if (boost::starts_with(url, "s3://")) {
    // the S3 file type currently works by download/uploading a local file
    // i.e. the s3_stream simply remaps a local file stream
    type = STD;
    if (is_output_stream) {
      output_stream = std::make_shared<s3_fstream>(url, true);
    } else {
      auto s3stream = std::make_shared<s3_fstream>(url, false);
      input_stream = (*s3stream)->get_underlying_stream();
      if (input_stream == nullptr) input_stream = s3stream;
      m_file_size = (*s3stream)->file_size();
    }
  } else {
    // must be local file
    if (is_output_stream) {
      // Output stream must be a local openable file.
      output_stream.reset(new std::ofstream(url, std::ofstream::binary));
      if (!output_stream->good()) {
        output_stream.reset();
        log_and_throw_io_failure("Cannot open " + url + " for writing");
      }
    } else {
      url = file_download_cache::get_instance().get_file(url);
      input_stream.reset(new std::ifstream(url, std::ifstream::binary));
      if (!input_stream->good()) {
        input_stream.reset();
        log_and_throw_io_failure("Cannot open " + url + " for reading");
      }
      // get the file size
      {
        std::ifstream fin;
        fin.open(url.c_str(), std::ifstream::binary);
        if (fin.good()) {
          fin.seekg(0, std::ios::end);
          m_file_size = fin.tellg();
        }
      }
    }
  }

  if (is_output_stream) {
    ASSERT_TRUE(output_stream->good());
  } else {
    ASSERT_TRUE(input_stream->good());
  }
}

size_t union_fstream::file_size() {
  return m_file_size;
}

/// Destructor
union_fstream::~union_fstream() {
}

/// Returns the current stream type. Whether it is a HDFS stream or a STD stream
union_fstream::stream_type union_fstream::get_type() const {
  return type;
}

std::shared_ptr<std::istream> union_fstream::get_istream () {
  ASSERT_TRUE(input_stream != NULL);
  return input_stream;
}

std::shared_ptr<std::ostream> union_fstream::get_ostream() {
  ASSERT_TRUE(output_stream != NULL);
  return output_stream;
}

/**
 * Returns the filename used to construct the union_fstream
 */
std::string union_fstream::get_name() const {
  return url;
}

} // namespace graphlab

