/*
* Copyright (C) 2016 Turi
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Affero General Public License as
* published by the Free Software Foundation, either version 3 of the
* License, or (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU Affero General Public License for more details.
*
* You should have received a copy of the GNU Affero General Public License
* along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/
#ifndef TEST_TEST_OBJECT_BASE_HPP
#define TEST_TEST_OBJECT_BASE_HPP
#include <string>
#include <cppipc/cppipc.hpp>
#include <cppipc/magic_macros.hpp>
GENERATE_INTERFACE_AND_PROXY(test_object_base,  test_object_proxy, 
                              (std::string, ping, (std::string))
                              (std::string, return_big_object, (size_t))
                              (int, add_one, (int)(std::string))
                              (int, add, (int)(int))
                              (int, subtract, (int)(int))
                              (int, return_one, )
                              (void, set_value, (size_t))
                              (size_t, get_value, )
                              (void, subtract_from, (std::shared_ptr<test_object_base>))
                              (void, swap, (std::shared_ptr<test_object_base>))
                              (std::shared_ptr<test_object_base>, operator-, (std::shared_ptr<test_object_base>))
                              (std::shared_ptr<test_object_base>, operator+, (std::shared_ptr<test_object_base>))
                              (void, an_exception, )
                            )

// the above code is equivalent
//
// class test_object_base {
//  public:
//   virtual std::string ping(std::string) = 0;
//   virtual int add_one(int) = 0;
//   virtual int add(int, int) = 0;
// 
//   virtual ~test_object_base() { }
// 
//   REGISTRATION_BEGIN(test_object_base)
//   REGISTER(test_object_base::ping)
//   REGISTER(test_object_base::add_one)
//   REGISTER(test_object_base::add)
//   REGISTRATION_END
// };
//
// class test_object_proxy:public test_object_base {
//  public:
//   cppipc::object_proxy<test_object_base> proxy;
//
//   inline test_object_proxy(cppipc::comm_client& comm):proxy(comm){ } 
//   inline std::string ping(std::string s) {
//     return proxy.call(&test_object_base::ping, s);
//   }
//   inline int add_one(int s) {
//     return proxy.call(&test_object_base::add_one, s);
//   }
//   inline int add(int a, int b) {
//     return proxy.call(&test_object_base::add, a, b);
//   }
// };



class test_object_impl:public test_object_base {
 public:
  size_t value;
  test_object_impl() {
    std::cout << "test_object_impl created\n";
  }
  ~test_object_impl() {
    std::cout << "test_object_impl destroyed\n";
  }
  std::string ping(std::string s) {
    return s;
  }
  std::string return_big_object(size_t s) {
    return std::string(s, ' ');
  }
  int add_one(int s, std::string k) {
    return s + 1;
  }
  int add(int a, int b) {
    return a + b;
  }
  int subtract(int a, int b) {
    return a - b;
  }
  int return_one() {
    return 1;
  }

  void set_value(size_t i) {
    value = i;
  }
  size_t get_value() {
    return value;
  }
  void subtract_from(std::shared_ptr<test_object_base> other) {
    value -= other->get_value();
  }
  void swap(std::shared_ptr<test_object_base> other) {
    size_t tmp = other->get_value();
    other->set_value(get_value());
    set_value(tmp);
  }

  std::shared_ptr<test_object_base> operator-(std::shared_ptr<test_object_base> other) {
    std::shared_ptr<test_object_impl> newobj(new test_object_impl);
    newobj->set_value(get_value() - other->get_value());
    return std::dynamic_pointer_cast<test_object_base>(newobj);
  }

  std::shared_ptr<test_object_base> operator+(std::shared_ptr<test_object_base> other) {
    std::shared_ptr<test_object_impl> same_obj = std::dynamic_pointer_cast<test_object_impl>(other);
    same_obj->set_value(get_value() + same_obj->get_value());
    return std::dynamic_pointer_cast<test_object_base>(same_obj);
  }

  void an_exception() {
    throw "hello world!";
  }
};



#endif
