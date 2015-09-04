/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#ifndef CPPIPC_COMMON_ERROR_TYPES_HPP
#define CPPIPC_COMMON_ERROR_TYPES_HPP
#include <string>
#include <exception>
#include <typeinfo>
#include <export.hpp>
namespace graphlab {

/**
 * Subclass the std::bad_alloc with custom message.
 */
class EXPORT bad_alloc : public std::bad_alloc {

  std::string msg;

  public:
    bad_alloc(const std::string& msg) : msg(msg) {}
    virtual const char* what() const throw() {
      return msg.c_str();
    }
};

/**
 * Subclass the std::bad_cast with custom message.
 */
class EXPORT bad_cast : public std::bad_cast {

  std::string msg;

  public:
  bad_cast(const std::string& msg) : msg(msg) {}
  virtual const char* what() const throw() {
    return msg.c_str();
  }
};

} // cppipc
#endif
