#include <boost/filesystem.hpp>
#include <lambda/lambda_master.hpp>
#include <lambda/graph_pylambda_master.hpp>
#include <globals/globals.hpp>

namespace graphlab {
/**
 * Set the path to the pylambda_slave binary used for evaluate python lambdas parallel in separate processes.
 */
void init_pylambda_worker() {
  namespace fs = boost::filesystem;
  const char* python_executable_env = std::getenv("__GL_PYTHON_EXECUTABLE__");
  if (python_executable_env) {
    graphlab::GLOBALS_PYTHON_EXECUTABLE = python_executable_env;
    logstream(LOG_INFO) << "Python executable: " << graphlab::GLOBALS_PYTHON_EXECUTABLE << std::endl;
    ASSERT_MSG(fs::exists(fs::path(graphlab::GLOBALS_PYTHON_EXECUTABLE)),
               "Python executable is not valid path. Do I exist?");
  } else {
    logstream(LOG_WARNING) << "Python executable not set. Python lambdas may not be available" << std::endl;
  }

  std::string pylambda_worker_script;
  {
    const char* pylambda_worker_script_env = std::getenv("__GL_PYLAMBDA_SCRIPT__");
    if (pylambda_worker_script_env) {
      logstream(LOG_INFO) << "PyLambda worker script: " << pylambda_worker_script_env << std::endl;
      pylambda_worker_script = pylambda_worker_script_env;
      ASSERT_MSG(fs::exists(fs::path(pylambda_worker_script)), "PyLambda worker script not valid.");
    } else {
      logstream(LOG_WARNING) << "Python lambda worker script not set. Python lambdas may not be available" 
                             << std::endl;
    }
  }

  // Set the lambda_worker_binary_and_args
  graphlab::lambda::lambda_master::set_lambda_worker_binary(
      std::vector<std::string>{graphlab::GLOBALS_PYTHON_EXECUTABLE, pylambda_worker_script});
}

} // end of namespace graphlab
