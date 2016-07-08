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
/**  
 * Copyright (c) 2009 Carnegie Mellon University. 
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS
 *  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied.  See the License for the specific language
 *  governing permissions and limitations under the License.
 *
 * For more about this software visit:
 *
 *      http://www.graphlab.ml.cmu.edu
 *
 */


#include <fstream>
#include <vector>
#include <map>
#include <string>
#include <cstring>

#include <cxxtest/TestSuite.h>

#include <boost/unordered_set.hpp>
#include <boost/unordered_map.hpp>
#include <boost/iostreams/device/array.hpp>
#include <boost/iostreams/stream.hpp>

#include <util/any.hpp>
#include <serialization/serialization_includes.hpp>


using namespace graphlab;



struct A{
  int z;
  void save(oarchive &a) const {
    a << z;
  }
  void load(iarchive &a) {
    a >> z;
  }
};

class TestClass{
public:
  int i;
  int j;
  std::vector<int> k;
  A l;
  void save(oarchive &a) const {
    a << i << j << k << l;
  }
  void load(iarchive &a) {
    a >> i >> j >> k >> l;
  }
};


struct pod_class_1: public graphlab::IS_POD_TYPE {
  size_t x;
};

struct pod_class_2 {
  size_t x;
}; 
SERIALIZABLE_POD(pod_class_2);


// test class which uses prefixes rather than serialization
struct file_class {
  size_t x = 0;
  void save(oarchive& a) const {
    std::string prefix = a.get_prefix(); 
    {
      general_ofstream fout(prefix + ".pika1");
      oarchive oarc(fout);
      oarc << x;
      fout.close();
    }
    {
      general_ofstream fout(prefix + ".pika2");
      oarchive oarc(fout);
      oarc << x + 1;
      fout.close();
    }
  }
  void load(iarchive& a) {
    std::string prefix = a.get_prefix(); 
    {
      general_ifstream fin(prefix + ".pika1");
      iarchive iarc(fin);
      iarc >> x;
      fin.close();
    }
    {
      general_ifstream fin(prefix + ".pika2");
      iarchive iarc(fin);
      size_t y;
      iarc >> y;
      TS_ASSERT_EQUALS(x + 1, y);
      fin.close();
    }
  }
};

class SerializeTestSuite : public CxxTest::TestSuite {
public:

  // Look for the class TestClass() to see the most interesting tutorial on how to
  // use the serializer
  void test_basic_datatype(void) {
    char t1 = 'z';
    bool t2 = true;
    int t3 = 10;
    int t4 = 18345;
    long t5 = 30921233;
    long long t6 = (long long)(t5)*100;
    float t7 = 10.35;
    double t8 = 3.14156;
    const char *t9 = "hello world";
    const char * t10 = "blue";
    graphlab::any t11;
    t11 = size_t(10);
    char r1;
    bool r2;
    int r3;
    int r4;
    long r5;
    long long r6;
    float r7;
    double r8;
    char r9[100];
    char r10[10];
    graphlab::any r11;

    // serialize t1-10
    std::ofstream f;
    f.open("test.bin",std::fstream::binary);
    oarchive a(f);
    a << t1 << t2 << t3 << t4 << t5 << t6 << t7 << t8;
    serialize(a, t9, strlen(t9) + 1);
    serialize(a, t10, strlen(t10) + 1);
    a << t11;
    f.close();

    // deserialize into r1-10
    std::ifstream g;
    g.open("test.bin",std::fstream::binary);
    iarchive b(g);
    b >> r1 >> r2 >> r3 >> r4 >> r5 >> r6 >> r7 >> r8;
    deserialize(b, &r9, strlen(t9) + 1);
    deserialize(b, r10, strlen(t10) + 1);
    b >> r11;
    g.close();

    TS_ASSERT_EQUALS(t1, r1);
    TS_ASSERT_EQUALS(t2, r2);
    TS_ASSERT_EQUALS(t3, r3);
    TS_ASSERT_EQUALS(t4, r4);
    TS_ASSERT_EQUALS(t5, r5);
    TS_ASSERT_EQUALS(t6, r6);
    TS_ASSERT_EQUALS(t7, r7);
    TS_ASSERT_EQUALS(t8, r8);
    TS_ASSERT_SAME_DATA(t9, r9, strlen(t9) + 1);
    TS_ASSERT_SAME_DATA(t10, r10, strlen(t10) + 1);
    TS_ASSERT_EQUALS(r11.as<size_t>(), t11.as<size_t>());
  }

  void test_vector_serialization(void) {
    std::vector<int> v;
    for (int i = 0;i< 10; ++i) {
      v.push_back(i);
    }
    std::ofstream f;
    f.open("test.bin",std::fstream::binary);
    oarchive a(f);
    a << v;
    f.close();

    std::vector<int> w;
    std::ifstream g;
    iarchive b(g);
    g.open("test.bin",std::fstream::binary);
    b >> w;
    g.close();

    for (int i = 0;i< 10; ++i) {
      TS_ASSERT_EQUALS(v[i], w[i]);
    }
  }


  void test_class_serialization(void) {
    // create a test class
    TestClass t;
    t.i=10;
    t.j=20;
    t.k.push_back(30);

    //serialize
    std::ofstream f;
    f.open("test.bin",std::fstream::binary);
    oarchive a(f);
    a << t;
    f.close();
    //deserialize into t2
    TestClass t2;
    std::ifstream g;
    g.open("test.bin",std::fstream::binary);
    iarchive b(g);
    b >> t2;
    g.close();
    // check
    TS_ASSERT_EQUALS(t.i, t2.i);
    TS_ASSERT_EQUALS(t.j, t2.j);
    TS_ASSERT_EQUALS(t.k.size(), t2.k.size());
    TS_ASSERT_EQUALS(t.k[0], t2.k[0]);
  }

  void test_vector_of_classes(void) {
    // create a vector of test classes
    std::vector<TestClass> vt;
    vt.resize(10);
    for (int i=0;i<10;i++) {
      vt[i].i=i;
      vt[i].j=i*21;
      vt[i].k.resize(10);
      vt[i].k[i]=i*51;
    }

    //serialize
    std::ofstream f;
    f.open("test.bin",std::fstream::binary);
    oarchive a(f);
    a << vt;
    f.close();

    //deserialize into vt2
    std::vector<TestClass> vt2;
    std::ifstream g;
    g.open("test.bin",std::fstream::binary);
    iarchive b(g);
    b >> vt2;
    g.close();
    // check
    TS_ASSERT_EQUALS(vt.size(), vt2.size());
    for (size_t i=0;i<10;i++) {
      TS_ASSERT_EQUALS(vt[i].i, vt2[i].i);
      TS_ASSERT_EQUALS(vt[i].j, vt2[i].j);
      TS_ASSERT_EQUALS(vt[i].k.size(), vt2[i].k.size());
      for (size_t j = 0; j < vt[i].k.size(); ++j) {
        TS_ASSERT_EQUALS(vt[i].k[j], vt2[i].k[j]);
      }
    }
  }

  void test_vector_of_strings(void) {
    std::string x = "Hello world";
    std::string y = "This is a test";
    std::vector<std::string> v;
    v.push_back(x); v.push_back(y);

    std::ofstream f;
    f.open("test.bin",std::fstream::binary);
    oarchive a(f);
    a << v;
    f.close();

    //deserialize into vt2
    std::vector<std::string> v2;
    std::ifstream g;
    g.open("test.bin",std::fstream::binary);
    iarchive b(g);
    b >> v2;
    g.close();
    TS_ASSERT_EQUALS(v[0], v2[0]);
    TS_ASSERT_EQUALS(v[1], v2[1]);
  }

  void test_map_serialization(void) {
    std::map<std::string,int> v;
    v["one"] = 1;
    v["two"] = 2;
    v["three"] = 3;

    std::ofstream f;
    f.open("test.bin",std::fstream::binary);
    oarchive a(f);
    a << v;
    f.close();

    //deserialize into vt2
    std::map<std::string,int> v2;
    std::ifstream g;
    g.open("test.bin",std::fstream::binary);
    iarchive b(g);
    b >> v2;
    g.close();
    TS_ASSERT_EQUALS(v["one"], v2["one"]);
    TS_ASSERT_EQUALS(v["two"], v2["two"]);
    TS_ASSERT_EQUALS(v["three"], v2["three"]);
  }

  void test_repeated_array_serialization(void) {
    typedef std::map<int, int> intmap;
    std::vector<char> buffer;
    std::vector<size_t> sizes(5);
    std::cout << "Making maps =====================================" << std::endl;
    for(size_t i = 0; i < sizes.size(); ++i) {
      std::stringstream strm;
      oarchive arc(strm);
      intmap im;
      im[i] = i;
      im[10*i] = 10*i;
      if(i % 2 == 0) im[i+sizes.size()] = 3;
      for(intmap::const_iterator iter = im.begin(); iter != im.end(); ++iter) {
        std::cout << "[" << iter->first << ", " << iter->second << "]\t";
      }
      std::cout << std::endl;
      arc << im;
      std::string str(strm.str());
      sizes[i] = str.size();
      int index = buffer.size();
      buffer.resize(index + str.size());
      memcpy(&buffer[index], str.c_str(), str.size());
    }
    std::cout << "reading maps =====================================" << std::endl;
    namespace bio = boost::iostreams;
    typedef bio::stream<bio::array_source> icharstream;
  

    
    for(size_t i = 0, offset=0; i < sizes.size(); ++i) {
      icharstream strm(&buffer[offset], sizes[i]);
      offset += sizes[i];
      intmap im;
      iarchive arc(strm);
      arc >> im;
      for(intmap::const_iterator iter = im.begin(); iter != im.end(); ++iter) {
        std::cout << "[" << iter->first << ", " << iter->second << "]\t";
      }
      std::cout << std::endl;
    }

  }
  
  void test_boost_unordered_map(void) {
    boost::unordered_map<std::string, size_t> m;
    m["hello"] = 1;
    m["world"] = 2;
    std::ofstream f;
    f.open("test.bin",std::fstream::binary);
    oarchive a(f);
    a << m;
    f.close();

    boost::unordered_map<std::string, size_t> m2;
    std::ifstream g;
    iarchive b(g);
    g.open("test.bin",std::fstream::binary);
    b >> m2;
    g.close();

    TS_ASSERT_EQUALS(m["hello"], m2["hello"]);
    TS_ASSERT_EQUALS(m["world"], m2["world"]);
  }


  void test_boost_unordered_set(void) {
    boost::unordered_set<std::string> m;
    m.insert("hello");
    m.insert("world");
    std::ofstream f;
    f.open("test.bin",std::fstream::binary);
    oarchive a(f);
    a << m;
    f.close();

    boost::unordered_set<std::string> m2;
    std::ifstream g;
    iarchive b(g);
    g.open("test.bin",std::fstream::binary);
    b >> m2;
    g.close();

    TS_ASSERT(m2.find("hello") != m2.end());
    TS_ASSERT(m2.find("world") != m2.end());
  }
  
  void test_pod_method_1() {
    std::vector<pod_class_1> p1;
    for (size_t i = 0;i < 1000; ++i) {
        pod_class_1 p;
        p.x = i;
        p1.push_back(p);
    }
    
    std::ofstream f;
    f.open("test.bin",std::fstream::binary);
    oarchive a(f);
    a << p1;
    f.close();

    std::vector<pod_class_1> p2;
    
    std::ifstream g;
    iarchive b(g);
    g.open("test.bin",std::fstream::binary);
    b >> p2;
    g.close();

    for (size_t i = 0;i < 1000; ++i) {
        TS_ASSERT_EQUALS(p1[i].x, p2[i].x);
    }
  }
  
    void test_pod_method_2() {
    std::vector<pod_class_2> p1;
    for (size_t i = 0;i < 1000; ++i) {
        pod_class_2 p;
        p.x = i;
        p1.push_back(p);
    }
    
    std::ofstream f;
    f.open("test.bin",std::fstream::binary);
    oarchive a(f);
    a << p1;
    f.close();

    std::vector<pod_class_2> p2;
    
    std::ifstream g;
    iarchive b(g);
    g.open("test.bin",std::fstream::binary);
    b >> p2;
    g.close();

    for (size_t i = 0;i < 1000; ++i) {
        TS_ASSERT_EQUALS(p1[i].x, p2[i].x);
    }
  }

  void test_directory_serialization() {
    // clean up for the test
    fileio::delete_path_recursive("test_dir");
    // data to serialize
    std::string hello = "hello world";
    std::vector<file_class> f;
    f.resize(4);
    for (size_t i = 0;i < f.size(); ++i) f[i].x = i;
    // write it out
    {
      dir_archive dirarc;
      dirarc.open_directory_for_write("test_dir", false);
      dirarc.set_metadata("pika", "chu");
      oarchive oarc(dirarc);
      oarc << hello << f;
    }
    // read it back
   {
      dir_archive dirarc;
      dirarc.open_directory_for_read("test_dir");
      std::string chu_expected;
      TS_ASSERT_EQUALS(dirarc.get_metadata("pika", chu_expected), true);
      TS_ASSERT_EQUALS(chu_expected, "chu");
      std::string ignored;
      TS_ASSERT_EQUALS(dirarc.get_metadata("mu", ignored), false);
      iarchive iarc(dirarc);
      std::string hello2;
      std::vector<file_class> f2;
      iarc >> hello2 >> f2;
      // validate
      TS_ASSERT_EQUALS(hello, hello2);
      TS_ASSERT_EQUALS(f.size(), f2.size());
      for (size_t i = 0;i < f.size(); ++i) {
        TS_ASSERT_EQUALS(f[i].x, f2[i].x);
      }
    }
    // make sure that open_directory_for_write with existing stuff will fail    
    {
      dir_archive dirarc;
      TS_ASSERT_THROWS_ANYTHING(dirarc.open_directory_for_write("test_dir", true));
    }
    // that I can overwrite with new data
    hello = "hello world2";
    f.resize(2);
    for (size_t i = 0;i < f.size(); ++i) f[i].x = 10 + i;
    {
      dir_archive dirarc;
      dirarc.open_directory_for_write("test_dir", false);
      oarchive oarc(dirarc);
      oarc << hello << f;
    }
    // read it back
    {
      dir_archive dirarc;
      dirarc.open_directory_for_read("test_dir");
      iarchive iarc(dirarc);
      std::string hello2;
      std::vector<file_class> f2;
      iarc >> hello2 >> f2;
      // validate
      TS_ASSERT_EQUALS(hello, hello2);
      TS_ASSERT_EQUALS(f.size(), f2.size());
      for (size_t i = 0;i < f.size(); ++i) {
        TS_ASSERT_EQUALS(f[i].x, f2[i].x);
      }
    }

    // that I can delete
    dir_archive::delete_archive("test_dir");
    
    // check that it no longer exists
    TS_ASSERT_EQUALS((int)fileio::get_file_status("test_dir"), 
                     (int)fileio::file_status::MISSING);

    // now make sure that trying to the file_class with a regular archive will
    // fail horribly.
    oarchive oarc;
    TS_ASSERT_THROWS_ANYTHING(oarc << f);
  }
};

