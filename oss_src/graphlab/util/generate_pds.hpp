/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
/*  
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


#include <iostream>
#include <vector>
namespace graphlab {
  class pds {
   public:
     pds() {}
     std::vector<size_t> get_pds(size_t p) {
       std::vector<size_t> result = find_pds(p);
       // verify pdsness
       size_t pdslength = p *p + p + 1;
       std::vector<size_t> count(pdslength, 0);
       for (size_t i = 0;i < result.size(); ++i) {
         for (size_t j = 0;j < result.size(); ++j) {
           if (i == j) continue;
           count[(result[i] - result[j] + pdslength) % pdslength]++;
         }
       }
       bool ispds = true;
       for (size_t i = 1;i < count.size(); ++i) {
         if (count[i] != 1) ispds = false;
       }

       // If success, return the result, else, return empty vector.
       if (ispds) {
         return result;
       } else {
         logstream(LOG_ERROR) << "Fail to generate pds for p = " << p << std::endl;
         return std::vector<size_t>();
       }
     }

   private:
     bool test_seq(size_t a, size_t b, size_t c, size_t p, std::vector<size_t>& result) {
       std::vector<size_t> seq;
       size_t pdslength = p*p + p + 1;
       seq.resize(pdslength + 3);
       seq[0] = 0; seq[1] = 0; seq[2] = 1;
       size_t ctr = 2;
       for (size_t i = 3; i < seq.size(); ++i) {
         seq[i] = a * seq[i - 1] + b * seq[i - 2] + c * seq[i - 3];
         seq[i] = seq[i] % p;
         ctr += (seq[i] == 0);
         // PDS must be of length p + 1
         // and are the 0's of seq.
         if (i < pdslength && ctr > p + 1) return false;
       }
       if (seq[pdslength] == 0 && seq[pdslength + 1] == 0){ 
         // we are good to go
         // now find the 0s
         for (size_t i = 0; i < pdslength; ++i) {
           if (seq[i] == 0) {
             result.push_back(i);
           }
         }
         // probably not necessary. but verify that the result has length p + 1
         if (result.size() != p + 1) {
           result.clear();
           return false;
         }
         return true;
       }
       else {
         return false;
       } 
     }


     std::vector<size_t> find_pds(size_t p) {
       std::vector<size_t> result;
       for (size_t a = 0; a < p; ++a) {
         for (size_t b = 0; b < p; ++b) {
           if (b == 0 && a == 0) continue;
           for (size_t c = 1; c < p; ++c) {
             if (test_seq(a,b,c,p,result)) {
               return result;
             }
           }
         }
       } 
       return result;
     }
  }; // end of pds class
} // end of graphlab namespace
