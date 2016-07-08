/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_SFRAME_COMMA_ESCAPE_STRING_HPP
#define GRAPHLAB_SFRAME_COMMA_ESCAPE_STRING_HPP

#include <string>
#include <iostream>

namespace graphlab {

constexpr char replace_char = '\x1F';
 
inline void comma_escape_string(const std::string& val, 
                   std::string& output, size_t& output_len) {
  if (output.size() < 2 * val.size()) {
    output.resize(2 * val.size());
  }
  char* cur_out = &(output[0]);
  // loop through the input string
  for (size_t i = 0; i < val.size(); ++i) {
    char c = val[i];
    switch(c) {
     case '\\':
         if (i < val.size() - 1 && (val[i+1] == 'u' || val[i+1] == 'x')) {
           (*cur_out++) = c;
         }else { 
           (*cur_out++) = '\\';
           (*cur_out++) = c;
         }
       break;
     case ',':
       (*cur_out++) = '\\';
       (*cur_out++) = replace_char;
       break;
     case '\'':
       (*cur_out++) = '\\';
       (*cur_out++) = '\'';
       break;
     case '\"':
         (*cur_out++) = '\\';
         (*cur_out++) = '\"';
       break;
     case '\t':
       (*cur_out++) = '\\';
       (*cur_out++) = 't';
       break;
     case '\r':
       (*cur_out++) = '\\';
       (*cur_out++) = 'r';
       break;
     case '\b':
       (*cur_out++) = '\\';
       (*cur_out++) = 'b';
       break;
     case '\n':
       (*cur_out++) = '\\';
       (*cur_out++) = 'n';
       break;
     default:
       (*cur_out++) = c;
   }
  }
  size_t len = cur_out - &(output[0]);
  output_len = len;
}


inline void comma_unescape_string(const std::string& val, 
                   std::string& output, size_t& output_len) {
  if (output.size() < val.size()) {
    output.resize(val.size());
  } 
  char* cur_out = &(output[0]);
  for (size_t i = 0; i < val.size(); ++i) {
    char c = val[i];
    switch(c) {
      case '\\':
        if (i < val.size() - 1 && (val[i+1] == '\\' )) {
          (*cur_out++) = '\\';
          i++;
        } 
        else if (i < val.size() - 1 && (val[i+1] == replace_char )) {
          (*cur_out++) = ',';
          i++;
        }
        else if (i < val.size() - 1 && (val[i+1] == '\'' )) {
          (*cur_out++) = '\'';
          i++;
        }
        else if (i < val.size() - 1 && (val[i+1] == '\"' )) {
          (*cur_out++) = '\"';
          i++;
        }
        else if (i < val.size() - 1 && (val[i+1] == 'n' )) {
          (*cur_out++) = '\n';
          i++;
        }
        else if (i < val.size() - 1 && (val[i+1] == 'b' )) {
          (*cur_out++) = '\b';
          i++;
        }
        else if (i < val.size() - 1 && (val[i+1] == 't' )) {
          (*cur_out++) = '\t';
          i++;
        }
        else if (i < val.size() - 1 && (val[i+1] == 'r' )) {
          (*cur_out++) = '\r';
          i++;
        }
        else 
          (*cur_out++) = c;
        break; 
      default:
        (*cur_out++) = c;  
    
    }
  }
  size_t len = cur_out - &(output[0]);
  output_len = len;
}

}
#endif
