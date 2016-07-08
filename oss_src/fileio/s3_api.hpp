/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef WEBSTOR_S3_UPLOADER_HPP
#define WEBSTOR_S3_UPLOADER_HPP
#include <string>
#include <fstream>
#include <future>
#include <memory>
#include <fileio/oss_webstor/wsconn.h>

namespace webstor {

// Endpoint addresses specified on http://docs.aws.amazon.com/general/latest/gr/rande.html
const std::vector<std::string> S3_END_POINTS {
  "s3-us-west-2.amazonaws.com",
  "s3-us-west-1.amazonaws.com",
  "s3-eu-west-1.amazonaws.com",
  "s3-ap-southeast-1.amazonaws.com",
  "s3-ap-southeast-2.amazonaws.com",
  "s3-ap-northeast-1.amazonaws.com",
  "s3-sa-east-1.amazonaws.com",
};

/**
 * A complete specification of an S3 bucket and object,
 * including all authentication required.
 */
struct s3url {
  std::string access_key_id;
  std::string secret_key;
  std::string bucket;
  std::string object_name;
  std::string endpoint;

  bool operator==(const s3url& other) {
    return access_key_id == other.access_key_id &&
           secret_key == other.secret_key &&
           bucket == other.bucket &&
           object_name == other.object_name &&
           endpoint == other.endpoint;
  }
};

/**
 * Uploads a local file to a target remote bucket with a given remote object
 * name. Returns a future object which will contain the response.  The input
 * file will be opened and will stay opened until the response is complete:
 * i.e. the file must not be overwritten, until the future is ready.
 *
 * \param file The local file to upload. The general_ifstream is used internally
 *             hence any file address which the general_ifstream supports, is
 *             supported. (i.e. HDFS addresses work). 
 * \param remote_bucket The remote S3 bucket to upload to
 * \param remote_object_name The name of the remote object.
 * \param access_key_id The AWS access key to use for authentication
 * \param secret_key The AWS secret key to use for authentication
 * \param proxy The proxy for network access, default empty
 * \param endpoint The particular endpoint host address, default is us-standard 
 *
 * \returns A future object containing a string. When read, an empty string
 * indicates success. Otherwise, it contains an error code.
 */
std::future<std::string> upload_to_s3(std::string local_file,
                                      std::string remote_bucket,
                                      std::string remote_object_name,
                                      std::string access_key_id,
                                      std::string secret_key,
                                      bool recursive = false,
                                      std::string proxy = "",
                                      std::string endpoint = "");


/**
 * Uploads a local file to a target remote bucket with a given remote object
 * name with the target specified as an S3 fully qualified URL of the form
 * s3://[access_key_id]:[secret_key]:[bucket]/[object_name]. This function 
 * basically decomposes the components of the URL and calls the other overload
 * of the upload_to_s3 function.
 *
 * \param local_file The local file to upload. The general_ifstream is used 
 *             internally hence any file address which the general_ifstream 
 *             supports, is supported. (i.e. HDFS addresses work). 
 * \param s3_url The S3 target to upload to. The URL must be of the form
 *               \b "s3://[access_key_id]:[secret_key]:[bucket]/[object_name]".
 *
 * \returns A future object containing a string. When read, an empty string
 * indicates success. Otherwise, it contains an error code.
 */
std::future<std::string> upload_to_s3(std::string local_file,
                                      std::string s3_url,
                                      std::string proxy = "",
                                      std::string endpoint = "");

/**
 * Recursive version of upload_to_s3 
 */
std::future<std::string> upload_to_s3_recursive(std::string local_file,
                                      std::string s3_url,
                                      std::string proxy = "",
                                      std::string endpoint = "");
/**
 * Downloads a remote S3 object to a local file. Returns a future object which
 * will contain the response. The output file will be opened and will stay
 * opened until the response is complete. 
 *
 * \param remote_bucket The remote S3 bucket to upload to
 * \param remote_object_name The name of the remote object.
 * \param local_file The local file to save to. The local file location must be 
 *                   x writable. The general_ofstream is used internally, hence
 *                   any file address which the general_ofstream supports, is
 *                   supported. (i.e. HDFS addresses work).
 * \param access_key_id The AWS access key to use for authentication
 * \param secret_key The AWS secret key to use for authentication
 * \param endpoint The particular endpoint host address, default is us-standard 
 *
 * \returns A future object containing a string. When read, an empty string
 * indicates success. Otherwise, it contains an error code.
 */
std::future<std::string> download_from_s3(std::string remote_bucket,
                                          std::string remote_object_name,
                                          std::string local_file,
                                          std::string access_key_id,
                                          std::string secret_key,
                                          bool recursive = false,
                                          std::string proxy = "",
                                          std::string endpoint = "");


/**
 * Downloads a remote S3 object as specified by a fully qualified S3 URL to a
 * local file. Returns a future object which will contain the response. The
 * output file will be opened and will stay opened until the response is
 * complete. 
 *
 * The S3 URL must be of the form
 * s3://[access_key_id]:[secret_key]:[bucket]/[object_name]. This function 
 * basically decomposes the components of the URL and calls the other overload
 * of the download_fom_s3 function.
 *
 * \param s3_url The S3 target to download from. The URL must be of the form
 *               \b "s3://[access_key_id]:[secret_key]:[bucket]/[object_name]".
 * \param local_file The local file to save to. The local file location must be 
 *                   x writable. The general_ofstream is used internally, hence
 *                   any file address which the general_ofstream supports, is
 *                   supported. (i.e. HDFS addresses work).
 *
 * \returns A future object containing a string. When read, an empty string
 * indicates success. Otherwise, it contains an error code.
 */
std::future<std::string> download_from_s3(std::string s3_url,
                                          std::string local_file,
                                          std::string proxy = "",
                                          std::string endpiont = "");

/**
 * Recursive version of download_from_s3
 */
std::future<std::string> download_from_s3_recursive(std::string s3_url,
                                          std::string local_file,
                                          std::string proxy = "",
                                          std::string endpiont = "");

/**
 * Get the last modified time stamp of file.
 *
 * Throw exception if the url cannot be fetched.
 *
 * Return empty string if last modified is not available,
 * e.g. the url is a directory path or file does not exist.
 */
std::string get_s3_file_last_modified(const std::string& url);


/**
 * Return type of list_objects;
 */
struct list_objects_response {
  /// Non-empty if there was an error
  std::string error;
  /// A list of all the "sub-directories" found
  std::vector<std::string> directories;

  /// A list of all the objects found.
  std::vector<std::string> objects;
  /// Last modified time for the objects.
  std::vector<std::string> objects_last_modified;
};

/**
 * Lists objects or prefixes prefixed by a give s3 url.
 *
 * This is a thin wrapper around the S3 API 
 * http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGET.html 
 * and may not quite do what you think it does.
 *
 * if s3_url points to a valid prefix, it will return only the prefix
 * as a directory. For instance if I have an S3 bucket containing
 * 
 * foo/hello.txt
 *
 * list_objects("s3://foo") will return simply "foo/" as a directory.
 *
 * See list_directory() and is_directory() for a more sensible implementation
 * which behaves somewhat more file system like.
 * 
 * \returns A list_objects_response object.
 * If list_objects_response.error is an empty string, it indicates success.
 * Otherwise, it contains an error code. list_objects_response.directories
 * indicate all "directories" stored with the requested prefix. And
 * list_objects_response.objects indicates all regular objects stored with the
 * requested prefix.
 *
 */
list_objects_response list_objects(std::string s3_url,
                                   std::string proxy = "");


/**
 * Lists all objects prefixed by a give s3 url.
 *
 * if s3_url points to a valid prefix, it will return the prefix's contents
 * like a directory.
 * 
 * foo/hello.txt
 *
 * list_objects("s3://foo") will return "foo/hello.txt" 
 *
 * If s3_url points to an object it will just return the object.
 * 
 * \returns A list_objects_response object.
 * If list_objects_response.error is an empty string, it indicates success.
 * Otherwise, it contains an error code. list_objects_response.directories
 * indicate all "directories" stored with the requested prefix. And
 * list_objects_response.objects indicates all regular objects stored with the
 * requested prefix.
 *
 */
list_objects_response list_directory(std::string s3_url,
                                     std::string proxy = "");

/**
 * Tests if url is a directory or a regular file.
 * Returns a pair of (exists, is_directory). If exists is false, 
 * is_directory should be ignored
 */
std::pair<bool, bool> is_directory(std::string s3_url,
                                   std::string proxy = "");



/**
 * Where url points to a single object, this deletes the object.
 * Returns an error string on success, and an empty string on failure.
 */
std::string delete_object(std::string s3_url,
                          std::string proxy = "");

/**
 * Where url points to a prefix, this deletes all objects with the
 * specified prefix.
 * Returns an error string on success, and an empty string on failure.
 */
std::string delete_prefix(std::string s3_url,
                          std::string proxy = "");

/**
 * Given an S3 URL of the form expected by parse_s3url,
 * this function drops the access_key_id and the secret_key from the string returning
 * s3://[bucket]/[object_name]
 *
 * If the url cannot be parsed, we try the best to remove information associated with ':'.
 *
 * If the url does not begin with s3://, return as is.
 */
std::string sanitize_s3_url(const std::string& url);

/**
 * This splits a URL of the form
 * s3://[access_key_id]:[secret_key]:[endpoint][/bucket]/[object_name]
 * into several pieces.
 *
 * endpoint and object_name are optional.
 *
 * Returns true on success, false on failure.
 */
bool parse_s3url(std::string url, s3url& ret);

/**
 * Set the timeout for upload.
 * \param timeout Timeout value in secs.
 */
void set_upload_timeout(long timeout);

/**
 * Set the timeout for download.
 * \param timeout Timeout value in secs.
 */
void set_download_timeout(long timeout);

/**
 * Return the S3 error code contains in the message.
 * If the message does not contain error code, return the message itself.
 */
std::string get_s3_error_code(const std::string& msg);


} // namespace webstor


#endif
