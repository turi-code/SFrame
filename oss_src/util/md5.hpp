#ifndef UTIL_MD5_HPP
#define UTIL_MD5_HPP
#include <string>
namespace graphlab {

/**
 * Computes the md5 checksum of a string, returning the md5 checksum in a 
 * hexadecimal string
 */
std::string md5(std::string val);

/**
 * Returns a 16 byte (non hexadecimal) string of the raw md5. 
 */
std::string md5_raw(std::string val);

} // namespace product_key
#endif
