/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_TOKEN_H_
#define GRAPHLAB_TOKEN_H_

#include <string>
#include <functional>
#include <serialization/oarchive.hpp>
#include <serialization/iarchive.hpp>
#include <flexible_type/flexible_type.hpp>

#include <util/int128_types.hpp>
#include <util/cityhash_gl.hpp>

namespace graphlab {

/**
 * Defines a Token class with a fixed, constant hash value.  Hashes
 * are calculated using the cityhash_gl hash functions.  It is
 * designed as a robust key for hash tables.
 *
 */
class Token {
 public:
  /**
   * Creates an empty Token with the 0 hash.
   */
  Token()
      : h_128(0)
  {
  }

  /**
   * Creates a Token from a flexible_type value.
   *
   * \param v   The flexible_type value.
   */
  explicit Token(const flexible_type& v)
      : h_128(v.hash128())
      , _value(v)
  {
  }

  /**
   * Creates a Token from a string keyword argument.
   *
   * \param s   The string value of the token.
   */
  explicit Token(const std::string& s)
      : h_128(hash128(s))
      , _value(s)
  {
  }
  
  /**
   * Creates a Token from a vector<char> argument.
   *
   * \param v   The vector of strings.
   */
  explicit Token(const std::vector<char>& v)
      : h_128(hash128(v.data(), v.size()))
      , _value(std::string(v.begin(), v.end()))
  {
  }

  /**
   * Creates a Token from a c-style string.
   *
   * \param s C-style string constant.
   */
  explicit Token(const char* s)
      : h_128(hash128(s, strlen(s)))
      , _value(s)
  {
  }

  /**
   * Creates a Token from an integer id.
   *
   * \param id Long integer value.
   */
  explicit Token(const long id)
      : h_128(hash128(id))
      , _value(id)
  {
  }


  // Generic operator interfaces
  inline bool operator==(const Token& t) const {
    return t.h_128 == h_128;
  }

  inline bool operator<(const Token& t) const {
    return t.h_128 < h_128;
  }

  /**
   * Returns the value of the token as a string.
   */
  std::string str() const {
    return _value;
  }

  /**
   * Returns a reference to the flexible type value underlying
   * everything
   */
  const flexible_type& value() const {
    return _value;
  }

  /**
   * Returns the 128 bit hash value of the token.
   */
  uint128_t hash() const {
    return h_128;
  }

  void save(oarchive &oarc) const {
    oarc << h_128 << _value;
  }

  void load(iarchive &iarc) {
    iarc >> h_128 >> _value;
  }

  template<class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
    ar & h_128; 
    ar & _value;
  }

 private:
  uint128_t h_128;
  flexible_type _value;
};

/**
 * Defines a Weak version of Token class with a fixed, constant hash
 * value.  Hashes are calculated using the cityhash_gl hash
 * functions.  It is designed as a robust key for hash tables.  The
 * difference between this class and the regular Token class is that
 * this one only stores the hash value, making it better suited for
 * querying over network connections. 
 *
 */
class WeakToken {
 public:
  /**
   * Creates an empty WeakToken object with the 0 hash.  
   */
  WeakToken()
      : h_128(0)
  {
  }

  WeakToken(const Token& t)
      : h_128(t.hash())
  {
  }

  WeakToken(const WeakToken& t1, const WeakToken& t2)
      : h_128(hash128_combine(t1.hash(), t2.hash()))
  {
  }
  
  /**
   * Creates a WeakToken from a flexible_type value.
   *
   * \param v   The flexible_type value.
   */
  WeakToken(const flexible_type& v)
      : h_128(v.hash128())
  {
  }


  /**
   * Creates a WeakToken from a string keyword argument.
   *
   * \param s   The string value of the token.
   */
  WeakToken(const std::string& s)
      : h_128(hash128(s))
  {
  }

  /**
   * Creates a WeakToken from a c-style string.
   *
   * \param s C-style string constant.
   */
  WeakToken(const char* s)
      : h_128(hash128(s, strlen(s)))
  {
  }

  /**
   * Creates a WeakToken from a vector<char> argument.
   *
   * \param v   The vector of strings.
   */
  WeakToken(const std::vector<char>& v)
      : h_128(hash128(v.data(), v.size()))
  {
  }
  
  /**
   * Creates a Token from a long integer id.
   *
   * \param id Long integer value.
   */
  WeakToken(const long id)
      : h_128(hash128(id))
  {
  }

  /**
   * Creates a Token from a size_t  id.
   *
   * \param id Long integer value.
   */
  WeakToken(const size_t id)
      : h_128(hash128(id))
  {
  }

  // Generic operator interfaces
  inline bool operator==(const WeakToken& t) const {
    return t.hash() == hash();
  }

  inline bool operator<(const WeakToken& t) const {
    return t.hash() < hash();
  }

  /**
   * Returns the 128 bit hash value of the token.
   */
  uint128_t hash() const {
    return h_128;
  }

  void save(oarchive &oarc) const {
    oarc << h_128;
  }

  void load(iarchive &iarc) {
    iarc >> h_128;
  }

 private:
  uint128_t h_128;
};
}

namespace std {

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wmismatched-tags"
#endif

template <> struct hash<graphlab::Token> {
  size_t operator()(const graphlab::Token& t) const {
    return size_t(t.hash());
  }
};
  
template <> struct hash<graphlab::WeakToken> {
  size_t operator()(const graphlab::WeakToken& t) const {
    return size_t(t.hash());
  }
};

#ifdef __clang__
#pragma clang diagnostic pop
#endif

}

#endif /* _TOKEN_H_ */
