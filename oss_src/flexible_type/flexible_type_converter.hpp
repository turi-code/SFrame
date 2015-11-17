/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */

#ifndef GRAPHLAB_FLEXIBLE_TYPE_FLEXIBLE_TYPE_CONVERTER_HPP
#define GRAPHLAB_FLEXIBLE_TYPE_FLEXIBLE_TYPE_CONVERTER_HPP
#include <vector>
#include <map>
#include <unordered_map>
#include <tuple>
#include <type_traits>
#include <util/code_optimization.hpp>
#include <flexible_type/flexible_type.hpp>
#include <flexible_type/type_traits.hpp>

namespace graphlab {

/**

   Type conversion priority:
   - Exact flexible_type types.
   - std::string (Allows casting from any flexible_type).
   - std::vector<double> (Allows casting from flex_vec or flex_list if flex_list okay).
   - gl_vector<T>, with T compatible with flexible_type (Allows casting from flex_vec or flex_list).
   - std::vector<T>, with T compatible with flexible_type (Allows casting from flex_vec or flex_list).
   - std::map<S, T>, with S, T compatible with flexible_type (Allows casting from flex_dict or flex_list of "pairs").
   - std::unordered_map<S, T>, with S, T compatible with flexible_type (Allows casting from flex_dict or flex_list of "pairs").
   - std::pair<S,T>, with S, T compatible with flexible_type.  (Allows casting from flex_list with 2 elements).
   - std::tuple<T...>, with all types numeric.  (Allows casting from flex_list or flex_vec with correct number of elements).
   - std::tuple<T...>, with all types compatible with flexible_type.  (Allows casting from flex_list/flex_vec with correct number of elements).
   - enum type.  Encoded as numeric.

*/

template <typename T> struct is_flexible_type_convertible;
template <typename T> static void convert_from_flexible_type(T& t, const flexible_type& f); 
template <typename T> static void convert_to_flexible_type(flexible_type& f, const T& t); 
template <typename T> static flexible_type convert_to_flexible_type(const T& t); 

namespace flexible_type_internals { 

static void throw_type_conversion_error(const flexible_type& val, const char *type) {
  std::ostringstream ss;
  ss << "Type conversion failure in flexible_type converter: expected "
     << type << "; got " << flex_type_enum_to_name(val.get_type());

  throw ss.str();
}

template <typename Arg>
static void __unpack_args(std::ostringstream& ss, Arg a) {
  ss << a;
}

template <typename Arg, typename... OtherArgs>
static void __unpack_args(std::ostringstream& ss, Arg a1, OtherArgs... a2) {
  ss << a1;
  __unpack_args(ss, a2...);
}

template <typename... PrintArgs>
static void throw_type_conversion_error(const flexible_type& val, const char* type, PrintArgs... args) {
  std::ostringstream ss;
  ss << "Type conversion failure in flexible_type converter: expected " << type;
  __unpack_args(ss, args...);
  ss << "; got " << flex_type_enum_to_name(val.get_type());

  throw ss.str();
}

struct ft_base_resolver {
  template <typename T> static constexpr bool matches() { return false; }
  template <typename T> static void get(T& t, const flexible_type& v) {
    static_assert(swallow_to_false<T>::value, "get with bad match");
  } 

  template <typename T>
  static void set(flexible_type& t, const T& v) {
    static_assert(swallow_to_false<T>::value, "set with bad match");
  } 
}; 

template <int> struct ft_converter : public ft_base_resolver { };

////////////////////////////////////////////////////////////////////////////////
// These are declared in terms of priority.  The resolver will match
// the first one in this list such that matches() is true.

/** flexible_type
 */
template <> struct ft_converter<1> : public ft_base_resolver {
  
  template <typename T> static constexpr bool matches() {
    return std::is_same<T, flexible_type>::value;
  }

  template <typename T> 
  static void get(T& dest, const flexible_type& src) {
    dest = src;
  }
  
  template <typename T> 
  static void set(flexible_type& dest, const T& src) {
    dest = src;
  }
};

/** Floating point values.
 */
template <> struct ft_converter<2> : public ft_base_resolver {
  
  template <typename T> static constexpr bool matches() {
    return std::is_floating_point<T>::value;
  }

  template <typename Float> 
  static void get(Float& dest, const flexible_type& src) {
    if(src.get_type() == flex_type_enum::FLOAT) {
      dest = static_cast<Float>(src.get<flex_float>());
    } else if(src.get_type() == flex_type_enum::INTEGER) {
      dest = static_cast<Float>(src.get<flex_int>());
    } else {
      throw_type_conversion_error(src, "numeric");
    }
  }

  template <typename Float> 
  static void set(Float& dest, const flexible_type& src) {
    dest = flex_float(src);
  }
};

/** Integer values.
 */
template <> struct ft_converter<3> : public ft_base_resolver {
  
  template <typename T> static constexpr bool matches() {
    return std::is_integral<T>::value;
  }

  template <typename Integer> 
  static void get(Integer& dest, const flexible_type& src) {
    if(src.get_type() == flex_type_enum::FLOAT) {
      flex_float v = src.get<flex_float>();
      if(static_cast<Integer>(v) != v) {
        throw_type_conversion_error(src, "integer / convertable float");
      }
    } else if(src.get_type() == flex_type_enum::INTEGER) {
      dest = static_cast<Integer>(src.get<flex_int>());
    } else {
      throw_type_conversion_error(src, "integer");
    }
  }

  template <typename Integer> 
  static void set(Integer& dest, const flexible_type& src) {
    dest = flex_float(src);
  }
};

/** flex_vec
 */
template <> struct ft_converter<4> : public ft_base_resolver {
  
  template <typename T> static constexpr bool matches() {
    return std::is_same<T, flex_vec>::value;
  }

  static void get(flex_vec& dest, const flexible_type& src) {
    if(src.get_type() == flex_type_enum::VECTOR) {
      dest = src.get<flex_vec>();
    } else if(src.get_type() == flex_type_enum::LIST) {
      const flex_list& f = src.get<flex_list>();
      dest.assign(f.begin(), f.end());
    } else {
      throw_type_conversion_error(src, "flex_vec");
    }
  }
  
  static void set(flexible_type& dest, const flex_vec& src) {
    dest = src; 
  }
};

/** flex_list
 */
template <> struct ft_converter<5> : public ft_base_resolver {
  
  template <typename T> static constexpr bool matches() {
    return std::is_same<T, flex_list>::value;
  }

  static void get(flex_list& dest, const flexible_type& src) {
    if(src.get_type() == flex_type_enum::LIST) {
      dest = src.get<flex_list>();
    } else if(src.get_type() == flex_type_enum::VECTOR) {
      const flex_vec& f = src.get<flex_vec>();
      dest.assign(f.begin(), f.end());
    } else {
      throw_type_conversion_error(src, "flex_list");
    }
  }
  
  static void set(flexible_type& dest, const flex_list& src) {
    dest = src; 
  }
};

////////////////////////////////////////////////////////////////////////////////
  
/** flex_dict and variants
 */
template <> struct ft_converter<6> : public ft_base_resolver {
  
  template <typename T> static constexpr bool matches() {
    typedef typename first_nested_type<T>::type pair_type;
    
    return ((is_gl_vector<T>::value || is_std_vector<T>::value)
            && is_std_pair<pair_type>::value
            && is_flexible_type_convertible<pair_type>::value);
  }

  template <typename T, typename U, template <typename...> class C>
  static void get(C<std::pair<T, U> >& dest, const flexible_type& src) {
    if(src.get_type() == flex_type_enum::DICT) {
      const flex_dict& fd = src.get<flex_dict>();
      dest.resize(fd.size());
      for(size_t i = 0; i < fd.size(); ++i) {
        convert_from_flexible_type(dest[i].first, fd[i].first);
        convert_from_flexible_type(dest[i].second, fd[i].second);
      }
    } else if(src.get_type() == flex_type_enum::LIST) {
      const flex_list& fl = src.get<flex_dict>();
      dest.resize(fl.size());
      for(size_t i = 0; i < fl.size(); ++i) {
        convert_from_flexible_type(dest[i], fl[i]);
      }
    } else {
      throw_type_conversion_error(src, "flex_dict or flex_list of 2-element list/vectors");
    }
  }

  template <typename T, typename U, template <typename...> class C>
  static void set(flexible_type& dest, const C<std::pair<T, U> >& src) {
    dest = flex_dict(src.size());
    for(size_t i = 0; i < src.size();++i) {
      dest[i] = std::make_pair(convert_to_flexible_type(src.first), convert_to_flexible_type(src.second));
    }
  }
};

/** flex_date_time
 */
template <> struct ft_converter<7> : public ft_base_resolver {
  
  template <typename T> static constexpr bool matches() {
    return std::is_same<T, flex_date_time>::value;
  }

  static void get(flex_date_time& dest, const flexible_type& src) {
    if(src.get_type() == flex_type_enum::DATETIME) {
      dest = src.get<flex_date_time>();
    } else {
      throw_type_conversion_error(src, "flex_date_time");
    }
  }

  static void set(flexible_type& dest, const flex_date_time& src) {
    dest = src; 
  }
};

// std::pair of flexible_type convertable stuff.
template <> struct ft_converter<8> : public ft_base_resolver {
  
  template <typename T> static constexpr bool matches() {
    
    return (is_std_pair<T>::value
            && std::is_arithmetic<typename first_nested_type<T>::type>::value
            && std::is_arithmetic<typename second_nested_type<T>::type>::value);
  }

  template <typename T, typename U>
  static void get(std::pair<T, U>& dest, const flexible_type& src) {
    if(src.get_type() == flex_type_enum::LIST) {
      const flex_list& l = src.get<flex_list>();
      if(l.size() != 2) {
        throw_type_conversion_error(src, "2-element flex_list/flex_vec (list size != 2)");
      }
      convert_from_flexible_type(dest.first, l[0]);
      convert_from_flexible_type(dest.second, l[1]);
    } else if(src.get_type() == flex_type_enum::VECTOR) {
      const flex_vec& v = src.get<flex_list>();
      if(v.size() != 2){
        throw_type_conversion_error(src, "2-element flex_list/flex_vec (vector size != 2)");
      } 
      dest = {v[0], v[1]};
    } else { 
      throw_type_conversion_error(src, "2-element flex_list/flex_vec");
    }
  }
  
  template <typename T, typename U>
  static void set(flexible_type& dest, const std::pair<T,U>& src,
                  typename std::enable_if<std::is_arithmetic<T>::value && std::is_arithmetic<U>::value>::type* = 0) {
    dest = flex_vec{flex_float(src.first), flex_float(src.second)};
  }

  template <typename T, typename U>
  static void set(flexible_type& dest, const std::pair<T,U>& src,
                  typename std::enable_if<!(std::is_arithmetic<T>::value
                                            && std::is_arithmetic<U>::value)>::type* = 0) {
    dest = flex_list{convert_to_flexible_type(src.first), convert_to_flexible_type(src.second)};
  }
};

// std::map<T, U> or std::unordered_map<T, U>, with T and U convertable to flexible_type.
template <> struct ft_converter<9> : public ft_base_resolver {
  
  template <typename T> static constexpr bool matches() {
    return ((is_std_map<T>::value || is_std_unordered_map<T>::value)
            && is_flexible_type_convertible<typename first_nested_type<T>::type>::value
            && is_flexible_type_convertible<typename second_nested_type<T>::type>::value);
  }

  template <typename T>
  static void get(T& dest, const flexible_type& src) {
    std::pair<typename T::key_type, typename T::value_type> p; 
    
    if(src.get_type() == flex_type_enum::DICT) {
      
      const flex_dict& fd = src.get<flex_dict>();
      
      for(size_t i = 0; i < fd.size(); ++i) {
        convert_from_flexible_type(p.first, fd[i].first);
        convert_from_flexible_type(p.second, fd[i].second);
        dest.insert(std::move(p));
      }
    } else if(src.get_type() == flex_type_enum::LIST) {
      const flex_list& l = src.get<flex_list>();
      for(size_t i = 0; i < l.size(); ++i) {
        convert_from_flexible_type(p, l[i]);
        dest.insert(std::move(p));
      }
    } else {
      throw_type_conversion_error(src, "flex_dict / list of 2-element flex_lists/flex_vec");
    }
  }

  template <typename T>
  static void set(flexible_type& dest, const T& src) {
    flex_dict fd;
    fd.reserve(src.size());
    for(const auto& p : src) {
      fd.push_back({convert_to_flexible_type(p.first), convert_to_flexible_type(p.second)});
    }
    dest = std::move(fd);
  }
};


namespace flexible_type_converter_impl {

struct _null_function { template <typename... A> void operator ()(A...){} }; 

////////////////////////////////////////////////////////////////////////////////

template <typename T>
static void __set_flexible_type(flexible_type& dest, const T& src) {
  convert_to_flexible_type(dest, src);
}

template <typename T>
static void __set_flexible_type(double& dest, const T& src) {
  dest = src;
}

template <typename C, typename... Args, size_t idx = 0>
static void unpack_tuple(C& dest, const std::tuple<Args...>& src) {

  struct _recurse {
    void operator ()(C& dest, const std::tuple<Args...>& src) {
      __set_flexible_type(dest[idx], src.template get<idx>());
      unpack_tuple<idx + 1>(dest, src);
    }
  };
  
  typename std::conditional<idx + 1 == sizeof...(Args), _null_function, _recurse>::type(dest, src);
}

////////////////////////////////////////////////////////////////////////////////


template <typename T>
static void __set_t(T& dest, const flexible_type& src) {
  convert_from_flexible_type(dest, src);
}

template <typename T>
static void __set_t(T& dest, double src) {
  dest = src;
}

template <typename C, typename... Args, size_t idx = 0>
static void pack_tuple(const std::tuple<Args...>& dest, C& src) {
    
  struct _recurse {
    void operator ()(const std::tuple<Args...>& dest, C& src) {
      __set_t(dest.template get<idx>(), src[idx]);
      pack_tuple<idx + 1>(dest, src);
    }
  };

  typename std::conditional<idx + 1 == sizeof...(Args), _null_function, _recurse>::type(dest, src);
}

} // flexible_type_converter_impl


// std::tuple<...> 
template <> struct ft_converter<10> : public ft_base_resolver {
  
  template <typename... T> static constexpr bool matches() {
    return (is_tuple<T...>::value && all_true<is_flexible_type_convertible, T...>::value);
  }
  
  template <typename... Args>
  static void get(std::tuple<Args...>& dest, const flexible_type& src) {
    switch(src.get_type()) {
      case flex_type_enum::LIST: { 
        const flex_list& d = src.get<flex_list>();

        if (d.size() != sizeof...(Args)) {
          std::string errormsg = 
              std::string("Expecting a list or vector of length ") 
              + std::to_string(sizeof...(Args)) + ", but we got a list of length " 
              + std::to_string(d.size());
          throw(errormsg);
        }
        flexible_type_converter_impl::pack_tuple(dest, d);
        break;
      }

      case flex_type_enum::VECTOR: { 
        const flex_vec& d = src.get<flex_vec>();
        if (d.size() != sizeof...(Args)) {
          std::string errormsg = 
              std::string("Expecting a list or vector of length ") 
              + std::to_string(sizeof...(Args)) + ", but we got a vector of length " 
              + std::to_string(d.size());
          throw(errormsg);
        }
        flexible_type_converter_impl::pack_tuple(dest, d);
        break;
      }
      default: { 
        std::string errormsg = 
            std::string("Expecting a list or vector of length ") 
            + std::to_string(sizeof...(Args)) + ", but we got a " 
            + flex_type_enum_to_name(src.get_type());
        throw(errormsg);
      }
    }
  }
    
  template <typename... Args>
  static void set(flexible_type& dest, const std::tuple<Args...> & src) {
    static constexpr bool is_numeric = all_true<std::is_arithmetic, Args...>::value;

    if(is_numeric) {
      flex_vec v(sizeof...(Args));
      flexible_type_converter_impl::unpack_tuple(v, src);
      dest = std::move(v);
    } else {
      flex_list v(sizeof...(Args));
      flexible_type_converter_impl::unpack_tuple(v, src);
      dest = std::move(v);
    }
  }
};

/**  Enum converter.
 */
template <> struct ft_converter<11> : public ft_base_resolver {
  
  template <typename T> static constexpr bool matches() {
    return std::is_enum<T>::value;
  }

  template <typename Enum> 
  static void get(Enum& dest, const flexible_type& src) {
    if(src.get_type() == flex_type_enum::INTEGER) {
      dest = static_cast<Enum>(src);
    } else {
      throw_type_conversion_error(src, "integer / enum.");
    }
  }

  template <typename Enum> 
  static void set(flexible_type& dest, const Enum& val) {
    dest = static_cast<flex_int>(val); 
  }
};

static constexpr bool last_converter_number = 11;


////////////////////////////////////////////////////////////////////////////////

template <int idx> struct ft_resolver {

  struct _true { template <typename T> static constexpr bool eval() { return true; } };

  struct _recurse_any_match {
    template <typename T> static constexpr bool eval() {
      return ft_resolver<idx - 1>::template any_match<T>();
    }
  };
  
  template <typename T> static constexpr bool any_match() {
    typedef typename std::conditional<
      ft_converter<idx>::template matches<T>(), _true, _recurse_any_match>::type cond_type;
    return cond_type::template eval<T>();
  }

  struct _recurse_matches {
    template <typename T> static constexpr bool eval() {
      return ft_resolver<idx - 1>::template matches<T>();
    }
  };
  
  template <typename T> static constexpr bool matches() {
    typedef typename std::conditional<
      (ft_converter<idx>::template matches<T>()
       && !(ft_resolver<idx - 1>::template any_match<T>())), _true, _recurse_matches>::type cond_type;
    return cond_type::template eval<T>();
  }

  struct _get {
    template <typename... A> void operator()(A... a) const { ft_converter<idx>::get(a...); }
  };

  struct _recurse_get {
    template <typename... A> void operator()(A... a) const { ft_resolver<idx - 1>::get(a...); }
  };
  
  template <typename T>
  static void get(T& t, const flexible_type& v) {
    typename std::conditional<matches<T>(), _get, _recurse_get>::type()(t, v);
  }

  struct _set {
    template <typename... A> void operator()(A... a) const { ft_converter<idx>::set(a...); }
  };

  struct _recurse_set {
    template <typename... A> void operator()(A... a) const { ft_resolver<idx - 1>::set(a...); }
  };
  
  template <typename T>
  static void set(flexible_type& t, const T& v) {
    typename std::conditional<matches<T>(), _set, _recurse_set>::type()(t, v);
  }
};


template <> struct ft_resolver<0> {
  template <typename T> static constexpr bool any_match() { return false; }
  template <typename T> static constexpr bool matches() { return false; }
  template <typename T> static void get(T& t, const flexible_type& v) { ASSERT_TRUE(false); }
  template <typename T> static void set(flexible_type& t, const T& v) { ASSERT_TRUE(false); } 
};

}

/**
 * is_flexible_type_convertible<T>::value is true if T can be converted
 * to and from a flexible_type via flexible_type_converter<T>.
 */
template <typename T>
struct is_flexible_type_convertible {
  static constexpr bool value = flexible_type_internals::ft_resolver<
    flexible_type_internals::last_converter_number >::template any_match<T>();
};

template <typename T> static void convert_from_flexible_type(T& t, const flexible_type& f) {
  flexible_type_internals::ft_resolver<flexible_type_internals::last_converter_number>::get(t, f);
};
  
template <typename T> static void convert_to_flexible_type(flexible_type& f, const T& t) {
  flexible_type_internals::ft_resolver<flexible_type_internals::last_converter_number>::set(f, t);
};

template <typename T> static flexible_type convert_to_flexible_type(const T& t) {
  flexible_type f;
  flexible_type_internals::ft_resolver<flexible_type_internals::last_converter_number>::set(f, t);
  return f;
};

template <typename T>
struct flexible_type_converter {

  static constexpr bool value = is_flexible_type_convertible<T>::value;

  flexible_type set(const T& t) const { return convert_to_flexible_type(t); }
  T get(const flexible_type& f) const {
    T t;
    convert_from_flexible_type(t, f);
    return t;
  }
}; 

                    
} // namespace graphlab 
#endif
