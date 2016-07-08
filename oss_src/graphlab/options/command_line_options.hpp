/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
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


#ifndef GRAPHLAB_COMMAND_LINE_OPTIONS
#define GRAPHLAB_COMMAND_LINE_OPTIONS

#include <string>
#include <vector>


#include <boost/program_options.hpp>


#include <graphlab/options/graphlab_options.hpp>


namespace boost {
  /**
    \ingroup util
    Converts a vector of any stream output-able type to a string
  */
  template<typename T>
  std::string graphlab_vec_to_string(const std::vector<T>& vec) {
    std::stringstream strm;
    strm << "{" ;
    for(size_t i = 0; i < vec.size(); ++i) {
      strm << vec[i];
      if(i < vec.size() - 1) strm << ", ";
    }
    strm << "}";
    return strm.str();
  }
  
  /**
   \ingroup util
   Provides lexical cast from vector<int> to string.
   Converts a vector of 1,2,3 to the string "{1, 2, 3}"
  */  
  template<>
  std::string lexical_cast< std::string>(const std::vector<int>& vec);

  /**
   \ingroup util
   Provides lexical cast from vector<int> to string.
   Converts a vector of 1,2,3 to the string "{1, 2, 3}"
  */  
  template<>
  std::string lexical_cast< std::string>(const std::vector<uint32_t>& vec);

  /**
   \ingroup util
   Provides lexical cast from vector<size_t> to string.
   Converts a vector of 1,2,3 to the string "{1, 2, 3}"
  */
  template<>
  std::string lexical_cast<std::string>(const std::vector<uint64_t>& vec);

  
  /**
   \ingroup util
   Provides lexical cast from vector<double> to string.
   Converts a vector of 1.1,2.2,3.3 to the string "{1.1, 2.2, 3.3}"
  */
  template<>
  std::string lexical_cast< std::string >(const std::vector<double>& vec);
  
  /**
   \ingroup util
   Provides lexical cast from vector<float> to string.
   Converts a vector of 1.1,2.2,3.3 to the string "{1.1, 2.2, 3.3}"
  */
  template<>
  std::string lexical_cast< std::string>(const std::vector<float>& vec);
  
  /**
   \ingroup util
   Provides lexical cast from vector<string> to string.
   Converts a vector of "hello", "world" to the string "{hello, world}"
  */
  template<>
  std::string lexical_cast< std::string>(const std::vector<std::string>& vec);

}; // end of namespace boost


namespace graphlab {
  /**
   * \ingroup util 
   *
   * \brief The GraphLab command line options class helps parse basic
   * command line options for the GraphLab framework as well as user
   * applications.
   *
   * Early in the development of GraphLab we realized that a lot of
   * time was spent writing code to parse the many GraphLab options as
   * well as each of the applications options.  In many cases we were
   * using the boost::program_options library which while very
   * powerful can also be fairly complicated.  
   *
   * As a consequence, we developed a simple command line options
   * object that parses the standard argv options capturing GraphLab
   * specific options and also processing users options.  GraphLab
   * command line tools to enable user applications to benefit from
   * sophisticated and still easy to use command line parsing.
   *
   * The command_line_options data-structure is built on top of the
   * boost::program_options library. We have tried to retain much of
   * the functionality of the boost::program_options library while
   * hiding some of the less "friendly" template meta-programming
   * "features".
   *
   *  Here is an example of how the library is used:
   *
   * \code
   * int main(int argc, char** argv) {
   *
   *   std::string filename;
   *   size_t dimensions = 20;
   *   double bound = 1E-5;
   *   bool use_x = false;
   *   std::vector<size_t> nsamples(1,10000);
   * 
   *   // Parse command line options
   *   graphlab::command_line_options clopts("Welcome to a the HelloWorld");
   *   clopts.attach_option("file", filename, "The input filename (required)");
   *   clopts.add_positional("file");
   *   clopts.attach_option("dim", dimensions,
   *                        "the dimension of the grid");
   *   clopts.attach_option("bound", bound,
   *                        "The termination bound");
   *   clopts.attach_option("usex", use_x,
   *                        "Use algorithm x");
   *   clopts.attach_option("nsamples", nsamples,
   *                        "A vector of the number of samples"); 
   *
   *   if(!clopts.parse(argc, argv)) return EXIT_FAILURE;
   * 
   *   if(!clopts.is_set("file")) {
   *     std::cout << "Input file not provided" << std::endl;
   *     clopts.print_description();
   *     return EXIT_FAILURE;
   *   }
   * }
   * \endcode
   *
   */
  class command_line_options : public graphlab_options {

    boost::program_options::options_description desc;
    boost::program_options::positional_options_description 
        pos_opts;
    size_t num_positional;
    boost::program_options::variables_map vm;
    
    bool suppress_graphlab_options;
   
    std::vector<std::string> unrecognized_options;

  public:

    /**
     * \brief Construct a command options object with basic settings.  
     *
     * \param [in] desc_str The description of the program that is
     * printed when --help is invoked (in addition to all the options
     * and their descriptions).
     *
     * \param [in] suppress_graphlab_options If set to true the
     * standard GraphLab options are not parsed and the help screen.
     * only presents the users options.  This is useful in cases where
     * command line options are needed outside of GraphLab binary
     * (e.g., simple utilities).
     */
    command_line_options(std::string desc_str,
                         bool suppress_graphlab_options = false) : 
      desc(desc_str), num_positional(0),
      suppress_graphlab_options(suppress_graphlab_options) {     
      // Add documentation for help
      namespace boost_po = boost::program_options;      
      desc.add_options()("help", "Print this help message.");
    } // End constructor



    
    /// Print the same message that is printed when the --help command
    /// line argument is provided.
    inline void print_description() const { std::cout << desc << std::endl; }


    /**
     * \brief This function should be called AFTER all the options
     * have been seen (including positionals). The parse function
     * reads the standard command line arguments and fills in the
     * attached variables. If there is an error in the syntax or
     * parsing fails the parse routine will print the error and return
     * false.
     *
     * If allow_unregistered is set to true, will permit unrecognized options
     */
    bool parse(int argc, const char* const* argv, 
               bool allow_unregistered = false);

    /** 
     * \brief The is set function is used to test if the user provided
     * the option.  The option string should match one of the attached
     * options.
     */
    bool is_set(const std::string& option);


    /**
     * If allow_unregistered flag is set on parse
     * this will contain the list of unrecognized options
     */
    inline std::vector<std::string> unrecognized() const {
      return unrecognized_options;
    }

    /**
     * \brief attach a user defined option to the command line options
     * parser.
     *
     * The attach option command is used to attach a user defined
     * option to the command line options parser.
     *
     * \param [in] option The name of the command line flag for that
     * option.
     *
     * \param [in,out] ret_var A reference to an "arbitrary" type
     * which can be any of the basic types (char, int, size_t, float,
     * double, bool, string...) or an std::vector of basic types. It
     * is important that the ret_cont point to a memory block that
     * will exist when parse is invoked.  The default value is read
     * from the ret_cont
     *                
     * \param [in] description Used to describe the option when --help is
     * called or when print_description is invoked.
     */
    template<typename T>
    void attach_option(const std::string& option,
                       T& ret_var,
                       const std::string& description) {
      namespace boost_po = boost::program_options;
      desc.add_options()
        (option.c_str(), boost_po::value<T>(&ret_var)->default_value(ret_var), 
         description.c_str());
    } // end of attach_option


    // /**
    // \brief attach a user defined option to the command line options
    // parser.
    
    // The attach option command is used to attach a user defined option
    // to the command line options parser.
    
    // \param option The name of the command line flag for that option.

    // \param ret_cont A pointer to an "arbitrary" type which can be any
    //                 of the basic types (char, int, size_t, float,
    //                 double, bool, string...) or an std::vector of
    //                 basic types. It is important that the ret_cont
    //                 point to a memory block that will exist when parse
    //                 is invoked.

    // \param default_value The default value of the parameter if the
    //                      user does not provide this parameter on the
    //                      command line.

    // \param description Used to describe the option when --help 
    //       is called or when print_description is invoked.
    // */
    // template<typename T>
    // void attach_option(const std::string& option,
    //                    T* ret_cont,
    //                    const T& default_value, 
    //                    const std::string& description) {
    //   namespace boost_po = boost::program_options;
    //   assert(ret_cont != NULL);
    //   desc.add_options()
    //     (option.c_str(),
    //      boost_po::value<T>(ret_cont)->default_value(default_value),
    //      description.c_str());
    // }
    
    /** 
     * \brief This function adds the option as a positional argument.
     * A positional argument does not require --option and instead is
     * read based on its location. Each add_positional call adds to
     * the next position. 
     */
    void add_positional(const std::string& str);

    
  }; // end class command line options


  




}; // end namespace graphlab


#endif

