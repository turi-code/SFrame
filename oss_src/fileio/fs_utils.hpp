/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

/**
 * This file implements a collection of routines that operate and behave
 * uniformly on all supported protocols. (currently, HDFS, S3, local fs)
 */
#ifndef GRAPHLAB_FILEIO_FS_UTILS_HPP
#define GRAPHLAB_FILEIO_FS_UTILS_HPP
#include <tuple>
#include <string>
#include <vector>
#include <fileio/sanitize_url.hpp>

namespace graphlab {
namespace fileio {

enum class file_status {
  MISSING, REGULAR_FILE, DIRECTORY, FS_UNAVAILABLE
};

/**
 * Checks a path (can be hdfs, s3, or regular) to see if it is a local path,
 * or a remote path.
 */
file_status get_file_status(const std::string& path);


/**
 * Enumerates the contents of a directory, listing all the files as well as
 * the file type. Path can be hdfs, s3, or regular filesystem.
 */
std::vector<std::pair<std::string, file_status>> get_directory_listing(const std::string& path);


/**
 * Creates a directory and all parent required directories (like mkdir -p).
 * Path can be hdfs, s3, or regular filesystem.
 * Returns true on success, false on failure.
 */
bool create_directory(const std::string& path);

/**
 * Try delete a given path. Path can be hdfs, s3, or regular filesystem.
 * If the path is a directory, then try remove all files under directory
 * If the path is a file, then file will be deleted immediately if the
 * file is not currently in use, otherwise the files are to be removed
 * later when the files are not used by anyone
 * If path doesn't exist, this returns true.
 * Returns true on success, false on failure.
 *
 * \param path The path to delete
 * \param status The file status if known. (Optional)
 */
bool delete_path(const std::string& path,
                 file_status status = file_status::FS_UNAVAILABLE);

/**
 * Deletes a path.
 * Internal function not meant to be called by external components
 *
 * If the path is a directory, then try remove all files under directory
 * If the path is a file, then file will be removed
 * If path doesn't exist, this returns true.
 * Returns true on success, false on failure.
 *
 * \param path The path to delete
 * \param status The file status if known. (Optional)
 */
bool delete_path_impl(const std::string& path,
                      file_status status = file_status::FS_UNAVAILABLE);

/**
 * Deletes a path. if path is a directory, deletion will delete
 * all files and directories it contains.
 * Path can be hdfs, s3, or regular filesystem.
 * If path doesn't exist, this returns true.
 * Returns true on success, false on failure.
 */
bool delete_path_recursive(const std::string& path);



// this is slightly out of place at the moment.


/**
 * A helper function to parse the hdfs url.
 * Return a tuple of host, port, and path.
 */
std::tuple<std::string, std::string, std::string> parse_hdfs_url(std::string url);


/**
 * Returns true if the protocol is writeable S3, HDFS, cache and local
 * filesystem; returns false otherwise.
 */
bool is_writable_protocol(std::string protocol);
/**
 * Returns true if the protocol is a protocol we will make curl handle.
 */
bool is_web_protocol(std::string protocol);
/**
 * Returns the protocol header. (everything before the ://).
 *
 * get_protocol("http://www.google.com") == "http"
 * get_protocol("s3://www.google.com") == "s3"
 * get_protocol("/root/test") == ""
 */
std::string get_protocol(std::string path);

/**
 * Returns the path removing the protocol header if there is one. .
 *
 * get_protocol("http://www.google.com") == "www.google.com"
 * get_protocol("s3://www.google.com") == "www.google.com"
 * get_protocol("/root/test") == "/root/test"
 */
std::string remove_protocol(std::string path);

/**
 * Extracts the file name from a fully qualified path.
 * So given: s3://bucket/data/123
 * This will return "123".
 *
 * In short, this will return everything to the right of the last trailing "/".
 */
std::string get_filename(std::string path);

/**
 * Extracts the directory name from a fully qualified path.
 * So given: s3://bucket/data/123
 * This will return "s3://bucket/data"
 *
 * In short, this will return everything to the left of the last trailing "/".
 */
std::string get_dirname(std::string path);

/**
 * Converts the path to a generic format for operation.
 *
 * Currently, all this means is that backslashes are converted to forward slashes.
 */
std::string convert_to_generic(const std::string &path);

/**
 * Given a root directory and an absolute path, tries to create a relative path
 * address between root_directory and the path; if not possible, returns the
 * the original path with no changes.
 *
 * This function is relatively limited. It will not add "../" structures to the
 * returned relative path. In other words, the path must point to a file/folder
 * inside of the root directory for this to return a relative path.
 *
 * Example:
 * make_relative_path("s3://bucket/data", "s3://bucket/data/123")
 * returns "123".
 *
 * make_relative_path("s3://bucket/data", "s3://foo/123")
 * returns "s3://foo/123".
 */
std::string make_relative_path(std::string root_directory, std::string path);

/**
 * Given a root directory and a relative path, tries to convert the relative
 * path to an absolute path. If the path is already an absolute path, returns
 * the original path with no changes.
 *
 * This function is absolutely limited. It will not handle "../" structures to
 * the returned relative path. In other words, the output path path must point
 * to a file/folder inside of the root directory.
 *
 * Example:
 * make_absolute_path("s3://bucket/data", "123")
 * returns "s3://bucket/data/123".
 *
 * make_absolute_path("s3://bucket/data", "s3://foo/123")
 * returns "s3://foo/123".
 */
std::string make_absolute_path(std::string root_directory, std::string path);

std::pair<std::string, std::string> split_path_elements(
    const std::string& url, file_status& status);

/**
 * Where URL is a glob of the form directory1/directory2/[glob]
 * (glob must only be on the file portion), returns a list of files matching
 * the glob pattern.
 */
std::vector<std::pair<std::string, file_status>> get_glob_files(
    const std::string& url);


/**
 * Given a URL, returns an ID value where URLs which return different ID values
 * are ok to be read in parallel, and URLs which return the same ID value
 * are probably sub-optimal if read in parallel; An ID of (size_t)(-1) indicates
 * that it can be read in parallel with everything.
 */
size_t get_io_parallelism_id(const std::string url);

/**
 * Returns true if the file can be opened. False otherwise.
 */
bool try_to_open_file(const std::string url);

/**
 * Copies a file from src to dest
 */
void copy(const std::string src, const std::string dest);

/**
 * Changes the file mode bits of the given file or directory in the url
 */
bool change_file_mode(const std::string path, short mode);

/**
 * Return canonical absolute path, eliminating dots, and symlinks
 */
std::string make_canonical_path(const std::string& path);

} // namespace fileio
} // namespace graphlab
#endif
