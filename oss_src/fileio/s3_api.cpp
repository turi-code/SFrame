/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef _WIN32
#include <arpa/inet.h>
#else
#include <ws2tcpip.h>
#endif
#include <memory>
#include <fstream>
#include <string>
#include <thread>
#include <future>
#include <regex>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/tokenizer.hpp>
#include <logger/logger.hpp>
#include <fileio/s3_api.hpp>
#include <fileio/fs_utils.hpp>
#include <fileio/general_fstream.hpp>
#include <fileio/run_aws.hpp>
#include <cppipc/server/cancel_ops.hpp>

namespace webstor {

namespace {

/*
 * Returns the future immediately.
 */
std::future<std::string> return_future_immediately(std::string value) {
    std::promise<std::string> retpromise;
    retpromise.set_value(value);
    return retpromise.get_future();
}

/**
 * Check the string is a valid s3 bucket name using the following criteria from
 * http://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html:
 *
 * 1 Bucket names must be at least 3 and no more than 63 characters long.
 * 2 Bucket names must be a series of one or more labels.
 * 3 Adjacent labels are separated by a single period (.).
 * 4 Bucket names can contain lowercase letters, numbers, and hyphens.
 * 5 Each label must start and end with a lowercase letter or a number.
 * 6 Bucket names must not be formatted as an IP address (e.g., 192.168.5.4).
 *
 * Amendment 1:
 *   Uppercase letters are in fact fine... And it is in fact case sensitive.
 *   Our test bucket Graphlab-Datasets breaks a couple of the rules above.
 *   Tweaked to accept capital letters.
 */
bool bucket_name_valid(const std::string& bucket_name) {

  // rule 1
  if (bucket_name.size() < 3 || bucket_name.size() > 63) {
    return false;
  }

  // rule 2, 3
  typedef boost::tokenizer<boost::char_separator<char> > tokenizer;
  boost::char_separator<char> sep(".");
  tokenizer labels(bucket_name, sep);
  tokenizer::iterator iter = labels.begin();
  if (iter == labels.end()) {
    return false;
  }

  // rule 4, 5
  auto label_valid = [](const std::string& label) {
    if (label.empty()) return false;
    using namespace std::regex_constants; 

    auto alnum = [=](char x) { return (x <= 'Z' && x >= 'A') || (x <= 'z' && x >= 'a') || (x <= '9' && x >= '0'); };
    auto alnum_or_hypen = [=](char x) { return x == '-' || alnum(x); };

    // begin
    if (!alnum(*label.begin())) return false;
    // end
    if (!alnum(*(label.end()-1))) return false;
    // everything in between
    for (size_t i = 1; i < label.size() - 1; ++i) {
      if (!alnum_or_hypen(label[i])) return false;
    }
    return true;
  };
  while (iter != labels.end()) {
    if (!label_valid(*iter)) return false;
    ++iter;
  }

  // rule 6, to validate, let's try creating an ip address from the bucket name.
  struct sockaddr_in sa;
  int result = inet_pton(AF_INET, bucket_name.c_str(), &(sa.sin_addr));
  if (result != 0) return false;

  return true;
}


std::string string_from_s3url(const s3url& parsed_url) {
  std::string ret = "s3://" + parsed_url.access_key_id + ":" 
      + parsed_url.secret_key + ":";
  if (!parsed_url.endpoint.empty()) {
    ret += parsed_url.endpoint + "/";
  }
  ret += parsed_url.bucket;
  if (!parsed_url.object_name.empty()) {
    ret += "/" + parsed_url.object_name;
  }
  return ret;
}

} // anonymous namespace

/**
 * This splits a URL of the form
 * s3://[access_key_id]:[secret_key]:[endpoint/][bucket]/[object_name]
 * into several pieces.
 *
 * Returns true on success, false on failure.
 */
bool parse_s3url(std::string url, s3url& ret) {
  // must begin with s3://
  if (!boost::algorithm::starts_with(url, "s3://")) {
    return false;
  }
  // strip the s3://
  url = url.substr(5);

  // Extract the access key ID and secret key
  size_t splitpos = url.find(':');
  if (splitpos == std::string::npos) {
    logstream(LOG_WARNING) << "Cannot find AWS_ACCESS_KEY_ID in the s3 url." << std::endl;
    return false;
  } else {
    ret.access_key_id = url.substr(0, splitpos);
    url= url.substr(splitpos + 1);
  }
  // Extract the secret key
  splitpos = url.find(':');
  if (splitpos == std::string::npos) {
    logstream(LOG_WARNING) << "Cannot find SECRET_AWS_ACCESS_KEY in the s3 url." << std::endl;
    return false;
  } else {
    ret.secret_key= url.substr(0, splitpos);
    url= url.substr(splitpos + 1);
  }

  // The rest is parsed using boost::tokenizer
  typedef boost::tokenizer<boost::char_separator<char> > 
    tokenizer;
  boost::char_separator<char> sep("/");
  tokenizer tokens(url, sep);
  tokenizer::iterator iter = tokens.begin();
  if (iter == tokens.end()) {
    return false;
  }

  // Parse endpoints
  if (std::regex_match (*iter, std::regex("(s3)(.*)(amazonaws.com)"))) {
    ret.endpoint = *iter;
    ++iter;
  }

  // Parse bucket name
  if (iter == tokens.end()) {
    return false;
  }
  if (!bucket_name_valid(*iter)) {
    logstream(LOG_WARNING) << "Invalid bucket name: " << *iter << std::endl;
    return false;
  }
  ret.bucket = *iter;
  ++iter;

  // The rest part is the object key
  if (iter == tokens.end()) {
    // no object key
    return true;
  }
  ret.object_name = *iter;
  ++iter;
  while(iter != tokens.end()) {
    ret.object_name += "/" + *iter;
    ++iter;
  }

  // std::cout << "Access Key: " << ret.access_key_id << "\n"
  //           << "Secret Key: " << ret.secret_key<< "\n"
  //           << "Bucket: " << ret.bucket<< "\n"
  //           << "Object: " << ret.object_name<< "\n"
  //           << "Endpoint: " << ret.endpoint << "\n";
  return true;
}


// The options we pass to aws cli for s3 commands
// "us-east-1" is the us-standard and it works with buckets from all regions
// "acl" grants the bucket owner full permission regardless of the uploader's account
static const std::string S3_COMMAND_OPTION = "--region us-east-1 --acl bucket-owner-full-control";


std::string validate_input_file(const std::string& local_file) {
  // Try to open the input file
  std::shared_ptr<graphlab::general_ifstream> fin(
      new graphlab::general_ifstream(local_file.c_str(),
                                     false)); // gzip_compressed.
                                             // We avoid decompressing the file 
                                             // on transfer. i.e. if the file is 
                                             // compressed/uncompressed to begin 
                                             // with, lets  keep it that way.

  // file cannot be opened
  if (!fin->good()) {
    return std::string("File ") + local_file + " cannot be opened.";
  }

  // get the file size. Return failure on failure.
  size_t file_size = fin->file_size();
  if (file_size == (size_t)(-1)) {
    return std::string("Size of file ") + local_file + " cannot be obtained.";
  }
  return "";
}

std::string validate_output_file(const std::string& local_file) {
  // Try to open the output file
  std::shared_ptr<graphlab::general_ofstream> fout( 
      new graphlab::general_ofstream(local_file.c_str(), 
                                     false));// gzip_compressed.
                                             // We avoid recompressing the file 
                                             // on transfer. i.e. if the file is 
                                             // compressed/uncompressed to begin 
                                             // with, lets  keep it that way.
  // file cannot be opened
  if (!fout->good()) {
    // return a failure immediately.
    return std::string("File ") + local_file + " cannot be opened.";
  }
  return "";
}

/**
 * Adding single quote around the path, and escape all single quotes inside the path.
 */
std::string quote_and_escape_path(const std::string& path) {
  // s3 keys are at most 1024 bytes,
  // we make the buffer three times bigger
  // and it should be enough to conver the length of escaped path s3://bucket_name/key
  const size_t BUF_SIZE = 1024 * 3;
  char* buf = new char[BUF_SIZE];
  size_t current_pos = 0;
  buf[current_pos++] = '\"'; // begin quote
  for (const auto& c : path) {
    if (c == '\'') {
      buf[current_pos++] = '\\'; // escape quote
      if (current_pos >= BUF_SIZE) { 
        delete[] buf;
        throw("Invalid path: exceed length limit");
      }
    }
    buf[current_pos++] = c;
    if (current_pos >= BUF_SIZE) {
      delete[] buf;
      throw("Invalid path: exceed length limit");
    }
  }
  buf[current_pos++] = '\"'; // end quote
  std::string ret(buf, current_pos);
  delete[] buf;
  return ret;
}


std::future<std::string> upload_to_s3(std::string local_file,
                                      std::string remote_bucket,
                                      std::string remote_object_name,
                                      std::string access_key_id,
                                      std::string secret_key,
                                      bool recursive,
                                      std::string proxy,
                                      std::string endpoint) {
  if (recursive) {
    return std::async(std::launch::async,
                     [=]()->std::string{
                       std::stringstream remote_path;
                       remote_path << "s3://" << remote_bucket
                                   << "/" << remote_object_name;
                       const std::vector<std::string> arglist{
                         "s3", "cp", "--recursive",
                         quote_and_escape_path(local_file),
                         quote_and_escape_path(remote_path.str()), S3_COMMAND_OPTION
                       };
                       return graphlab::fileio::run_aws_command(arglist, access_key_id, secret_key);
                     });

  } else {
    std::string validate_message = validate_input_file(local_file);

    if (!validate_message.empty())
      return return_future_immediately(validate_message);

    return std::async(std::launch::async,
                     [=]()->std::string{
                       std::stringstream remote_path;
                       remote_path << "s3://" << remote_bucket
                                   << "/" << remote_object_name;
                       const std::vector<std::string> arglist{
                         "s3", "cp",
                         quote_and_escape_path(local_file),
                         quote_and_escape_path(remote_path.str()),
                         S3_COMMAND_OPTION
                       };
                       return graphlab::fileio::run_aws_command(arglist, access_key_id, secret_key);
                     });
  }
}


std::future<std::string> upload_to_s3(std::string local_file,
                                      std::string url,
                                      std::string proxy,
                                      std::string endpoint) {
  s3url parsed_url;
  bool success = parse_s3url(url, parsed_url);
  if (!success || parsed_url.object_name.empty()) return return_future_immediately("Malformed URL");

  return upload_to_s3(local_file,
                      parsed_url.bucket,
                      parsed_url.object_name,
                      parsed_url.access_key_id,
                      parsed_url.secret_key,
                      false,
                      proxy,
                      endpoint.empty() ? parsed_url.endpoint : endpoint);
}

std::future<std::string> upload_to_s3_recursive(std::string local_file,
                                                std::string url,
                                                std::string proxy,
                                                std::string endpoint) {
  s3url parsed_url;
  bool success = parse_s3url(url, parsed_url);
  if (!success || parsed_url.object_name.empty()) return return_future_immediately("Malformed URL");

  return upload_to_s3(local_file,
                      parsed_url.bucket,
                      parsed_url.object_name,
                      parsed_url.access_key_id,
                      parsed_url.secret_key,
                      true,
                      proxy,
                      endpoint.empty() ? parsed_url.endpoint : endpoint);
}


std::future<std::string> download_from_s3(std::string remote_bucket,
                                          std::string remote_object_name,
                                          std::string local_file,
                                          std::string access_key_id,
                                          std::string secret_key,
                                          bool recursive,
                                          std::string proxy,
                                          std::string endpoint) {

  if (recursive) {
    return std::async(std::launch::async, 
                     [=]()->std::string{
                       std::stringstream remote_path;
                       remote_path << "s3://" << remote_bucket
                                   << "/" << remote_object_name;
                       const std::vector<std::string> arglist{
                         "s3", "cp", "--recursive",
                         quote_and_escape_path(remote_path.str()),
                         quote_and_escape_path(local_file),
                         S3_COMMAND_OPTION
                       };
                       return graphlab::fileio::run_aws_command(arglist, access_key_id, secret_key);
                     });
  } else {
    std::string validate_message = validate_output_file(local_file);
    if (!validate_message.empty())
      return return_future_immediately(validate_message);

    return std::async(std::launch::async, 
                     [=]()->std::string{
                       std::stringstream remote_path;
                       remote_path << "s3://" << remote_bucket
                                   << "/" << remote_object_name;
                       const std::vector<std::string> arglist{
                         "s3", "cp",
                         quote_and_escape_path(remote_path.str()),
                         quote_and_escape_path(local_file),
                         S3_COMMAND_OPTION
                       };
                       return graphlab::fileio::run_aws_command(arglist, access_key_id, secret_key);
                     });
  }
}

std::future<std::string> download_from_s3(std::string url,
                                          std::string local_file,
                                          std::string proxy,
                                          std::string endpoint) {
  s3url parsed_url;
  bool success = parse_s3url(url, parsed_url);
  if (!success || parsed_url.object_name.empty()) return return_future_immediately("Malformed URL");

  return download_from_s3(parsed_url.bucket,
                          parsed_url.object_name,
                          local_file,
                          parsed_url.access_key_id,
                          parsed_url.secret_key,
                          false,
                          proxy,
                          endpoint.empty() ? parsed_url.endpoint : endpoint);
}

std::future<std::string> download_from_s3_recursive(std::string url,
                                                    std::string local_file,
                                                    std::string proxy,
                                                    std::string endpoint) {
  s3url parsed_url;
  bool success = parse_s3url(url, parsed_url);
  if (!success || parsed_url.object_name.empty()) return return_future_immediately("Malformed URL");

  return download_from_s3(parsed_url.bucket,
                          parsed_url.object_name,
                          local_file,
                          parsed_url.access_key_id,
                          parsed_url.secret_key,
                          true,
                          proxy,
                          endpoint.empty() ? parsed_url.endpoint : endpoint);
}



list_objects_response list_objects_impl(s3url parsed_url,
                                        std::string proxy,
                                        std::string endpoint) {
  list_objects_response ret;
  // Setup the WebStor connection configuration
  WsConfig config = {};
  config.accKey = parsed_url.access_key_id.c_str();
  config.secKey = parsed_url.secret_key.c_str();
  config.storType = WST_S3;
  config.isHttps = true;
  config.port = NULL;
  config.proxy = proxy.c_str();
  config.host = endpoint.c_str();

  // create connection object.
  std::shared_ptr<WsConnection> conn(new WsConnection(config));
  try {
    std::vector<WsObject> listed_objects;
    conn->listAllObjects(parsed_url.bucket.c_str(), 
                         parsed_url.object_name.c_str(), 
                         "/", &listed_objects);
    for (size_t i = 0;i < listed_objects.size(); ++i) {
      if (listed_objects[i].isDir) {
        std::string key = listed_objects[i].key;
        // strip the ending "/" on a directory
        if (boost::ends_with(key, "/")) key = key.substr(0, key.length() - 1);
        ret.directories.push_back(key);
      } else {
        ret.objects.push_back(listed_objects[i].key);
        ret.objects_last_modified.push_back(listed_objects[i].lastModified);
      }
    }
  } catch(std::exception& except) {
    ret.error = except.what();
  } catch (const char* msg) {
    ret.error = std::string(msg);
  } catch (...) {
    ret.error = "Unknown Exception";
  }

  // put on the protocol prefixes
  for (auto& dir : ret.directories) {
    s3url dirurl = parsed_url;
    dirurl.object_name = dir;
    dir = string_from_s3url(dirurl);
  }
  for (auto& object: ret.objects) {
    s3url objurl = parsed_url;
    objurl.object_name = object;
    object = string_from_s3url(objurl);
  }
  return ret;
}


list_objects_response list_objects(std::string url,
                                   std::string proxy) {
  s3url parsed_url;
  list_objects_response ret;
  bool success = parse_s3url(url, parsed_url);
  if (!success) {
    ret.error = "Malformed URL";
    return ret;
  }

  size_t current_endpoint = 0;
  // try without an endpoint
  ret = list_objects_impl(parsed_url, proxy, "");
  // try all endpoints
  while (boost::algorithm::icontains(ret.error, "PermanentRedirect") && 
         current_endpoint < webstor::S3_END_POINTS.size()) {
    ret = list_objects_impl(parsed_url, proxy, S3_END_POINTS[current_endpoint]);
    ++current_endpoint;
  }
  return ret;
}


std::pair<bool, bool> is_directory(std::string url,
                                   std::string proxy) {
  s3url parsed_url;
  list_objects_response ret;
  bool success = parse_s3url(url, parsed_url);
  if (!success) {
    return {false, false};
  }
  // if there are no "/"'s it is just a top level bucket

  list_objects_response response = list_objects(url, proxy);
  // an error occured
  if (!response.error.empty()) {
    return {false, false};
  }
  // its a top level bucket name
  if (parsed_url.object_name.empty()) {
    return {true, true};
  }
  // is a directory
  for (auto dir: response.directories) {
    if (dir == url) {
      return {true, true};
    }
  }
  // is an object 
  for (auto object: response.objects) {
    if (object == url) {
      return {true, false};
    }
  }
  // is not found
  return {false, false};
}


list_objects_response list_directory(std::string url,
                                     std::string proxy) {
  s3url parsed_url;
  list_objects_response ret;
  bool success = parse_s3url(url, parsed_url);
  if (!success) {
    ret.error = "Malformed URL";
    return ret;
  }
  // normalize the URL so it doesn't matter if you put strange "/"s at the end
  url = string_from_s3url(parsed_url);
  std::pair<bool, bool> isdir = is_directory(url, proxy);
  // if not found.
  if (isdir.first == false) return ret;
  // if its a directory
  if (isdir.second) {
    // if there are no "/"'s it is a top level bucket and we don't need
    // to mess with prefixes to get the contents
    if (!parsed_url.object_name.empty()) {
      parsed_url.object_name = parsed_url.object_name + "/";
    }
    size_t current_endpoint = 0;
    // try without an endpoint
    ret = list_objects_impl(parsed_url, proxy, "");
    // try all endpoints
    while (boost::algorithm::icontains(ret.error, "PermanentRedirect") && 
           current_endpoint < webstor::S3_END_POINTS.size()) {
      ret = list_objects_impl(parsed_url, proxy, S3_END_POINTS[current_endpoint]);
      ++current_endpoint;
    }
  } else {
    ret.objects.push_back(url);
  }
  return ret;
}


/// returns an error string on failure. Empty string on success
std::string delete_object_impl(s3url parsed_url,
                               std::string proxy,
                               std::string endpoint) {
  std::string ret;
  // Setup the WebStor connection configuration
  WsConfig config = {};
  config.accKey = parsed_url.access_key_id.c_str();
  config.secKey = parsed_url.secret_key.c_str();
  config.storType = WST_S3;
  config.isHttps = true;
  config.port = NULL;
  config.proxy = proxy.c_str();
  config.host = endpoint.c_str();

  // create connection object.
  std::shared_ptr<WsConnection> conn(new WsConnection(config));
  try {
    conn->del(parsed_url.bucket.c_str(), 
              parsed_url.object_name.c_str()); 
  } catch(std::exception& except) {
    ret = except.what();
  } catch (const char* msg) {
    ret = std::string(msg);
  } catch (...) {
    ret = "Unknown Exception";
  }
  return ret;
}


/// returns an error string on failure. Empty string on success
std::string delete_prefix_impl(s3url parsed_url,
                               std::string proxy,
                               std::string endpoint) {
  std::string ret;
  // Setup the WebStor connection configuration
  WsConfig config = {};
  config.accKey = parsed_url.access_key_id.c_str();
  config.secKey = parsed_url.secret_key.c_str();
  config.storType = WST_S3;
  config.isHttps = true;
  config.port = NULL;
  config.proxy = proxy.c_str();
  config.host = endpoint.c_str();

  // create connection object.
  std::shared_ptr<WsConnection> conn(new WsConnection(config));
  try {
    conn->delAll(parsed_url.bucket.c_str(), 
              parsed_url.object_name.c_str()); 
  } catch(std::exception& except) {
    ret = except.what();
  } catch (const char* msg) {
    ret = std::string(msg);
  } catch (...) {
    ret = "Unknown Exception";
  }
  return ret;
}
std::string delete_object(std::string url,
                          std::string proxy) {
  s3url parsed_url;
  std::string ret;
  bool success = parse_s3url(url, parsed_url);
  if (!success) {
    ret = "Malformed URL";
    return ret;
  }
  // normalize the URL so it doesn't matter if you put strange "/"s at the end
  ret = delete_object_impl(parsed_url, proxy, "");
  // try all endpoints
  size_t current_endpoint = 0;
  while (boost::algorithm::icontains(ret , "PermanentRedirect") && 
         current_endpoint < webstor::S3_END_POINTS.size()) {
    ret = delete_object_impl(parsed_url, proxy, S3_END_POINTS[current_endpoint]);
    ++current_endpoint;
  }
  return ret;
}

std::string delete_prefix(std::string url,
                          std::string proxy) {
  s3url parsed_url;
  std::string ret;
  bool success = parse_s3url(url, parsed_url);
  if (!success) {
    ret = "Malformed URL";
    return ret;
  }
  // normalize the URL so it doesn't matter if you put strange "/"s at the end
  ret = delete_prefix_impl(parsed_url, proxy, "");
  // try all endpoints
  size_t current_endpoint = 0;
  while (boost::algorithm::icontains(ret , "PermanentRedirect") && 
         current_endpoint < webstor::S3_END_POINTS.size()) {
    ret = delete_prefix_impl(parsed_url, proxy, S3_END_POINTS[current_endpoint]);
    ++current_endpoint;
  }
  return ret;
}


std::string sanitize_s3_url_aggressive(std::string url) {
  // must begin with s3://
  if (!boost::algorithm::starts_with(url, "s3://")) {
    return url;
  }
  // strip the s3://
  url = url.substr(5);

  // strip the secret key and the access key following the usual rules.
  size_t splitpos = url.find(':');
  if (splitpos != std::string::npos) url = url.substr(splitpos + 1);
  splitpos = url.find(':');
  if (splitpos != std::string::npos) url = url.substr(splitpos + 1);

  // now, a user error is possible where ":" shows up inside the 
  // secret key / access key thus leaking part of a key in the logs.
  // so we also perform a more aggressive truncation.
  // find the first "/" and delete everything up to the last ":"
  // before the first "/"
  size_t bucketend = url.find('/');
  if (bucketend == std::string::npos) bucketend = url.length();
  size_t last_colon = url.find_last_of(':', bucketend);
  if (last_colon != std::string::npos) url = url.substr(last_colon + 1);
  return "s3://" + url;
}

std::string sanitize_s3_url(const std::string& url) {
  s3url parsed_url;
  if ( parse_s3url(url, parsed_url) ) {
    if (parsed_url.endpoint.empty())
      return "s3://" + parsed_url.bucket + "/" + parsed_url.object_name;
    else
      return "s3://" + parsed_url.endpoint + "/" + parsed_url.bucket + "/" + parsed_url.object_name;
  } else {
    return sanitize_s3_url_aggressive(url);
  };
}

std::string get_s3_error_code(const std::string& msg) {
  static const std::vector<std::string> errorcodes {
    "AccessDenied", "NoSuchBucket", "InvalidAccessKeyId",
    "InvalidBucketName", "KeyTooLong", "NoSuchKey", "RequestTimeout"
  };

  // User friendly error codes, return immediately. 
  for (const auto& ec : errorcodes) {
    if (boost::algorithm::icontains(msg, ec)) {
      return ec;
    }
  }

  // Error code that may need some explanation.
  // Add messages for error code below:
  // ... 
  // best guess for 403 error
  if (boost::algorithm::icontains(msg, "forbidden")) {
    return "403 Forbidden. Please check your AWS credentials and permission to the file.";
  }

  return msg;
}

std::string get_s3_file_last_modified(const std::string& url){
  list_objects_response response = list_objects(url);
  if (response.error.empty() && response.objects_last_modified.size() == 1) {
    return response.objects_last_modified[0];
  } else if (!response.error.empty()) {
    logstream(LOG_WARNING) << "List object error: " << response.error << std::endl;
    throw(response.error);
  }
  return "";
}

} // namespace webstor
