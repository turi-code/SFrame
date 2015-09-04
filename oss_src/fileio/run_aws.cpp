/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <fileio/run_aws.hpp>
#ifndef _WIN32
#include <sys/types.h>
#include <sys/wait.h>
#endif
#include <unistd.h>
#include <parallel/pthread_tools.hpp>
#include <fileio/temp_files.hpp>
#include <logger/logger.hpp>
#include <unistd.h>
#include <process/process.hpp>
#include <cppipc/server/cancel_ops.hpp>

namespace graphlab {
  namespace fileio {

static graphlab::mutex env_lock;

bool wait_on_child_and_print_progress (process &child_proc) {
  const size_t BUF_SIZE = 4096;
  char buf[BUF_SIZE];
  ssize_t bytes_read;
  bool success = true;
  while( (bytes_read = child_proc.read_from_child(buf, BUF_SIZE)) > 0 ) {
    logprogress_stream << std::string(buf, buf + bytes_read);
    if (cppipc::must_cancel()) {
      logprogress_stream << "Cancel by user" << std::endl;
      child_proc.kill(false);
      success = false;
      break;
    }
  }
  logprogress_stream << std::endl;

  return success;
}

std::string get_child_error_or_empty(const std::string& file) {
  std::ifstream fin(file, std::ifstream::binary);
  return std::string((std::istreambuf_iterator<char>(fin)),
                      std::istreambuf_iterator<char>());
}

std::string run_aws_command(const std::vector<std::string>& arglist,
                            const std::string& aws_access_key_id,
                            const std::string& aws_secret_access_key) {
  {
    std::lock_guard<graphlab::mutex> lock_guard(env_lock);
#ifndef _WIN32
    setenv("AWS_ACCESS_KEY_ID", aws_access_key_id.c_str(), 1 /*overwrite*/);
    setenv("AWS_SECRET_ACCESS_KEY", aws_secret_access_key.c_str(), 1 /*overwrite*/);
#else
    _putenv_s("AWS_ACCESS_KEY_ID", aws_access_key_id.c_str());
    _putenv_s("AWS_SECRET_ACCESS_KEY", aws_secret_access_key.c_str());
#endif
  }

  // Creates a temp file and redirect the child process's stderr
  std::string child_err_file = get_temp_name();

  std::stringstream command_builder;
  std::vector<std::string> argv;

  //TODO: Add a "launch_shell" function to process library
#ifndef _WIN32
  std::string cmd = "/bin/sh";

  argv.push_back("-c");

  // We put cd here because aws command prints url relative to the working
  // directory. Without cd, it will print out stuff like
  // download s3://foo to ../../../../../../../../../var/tmp/graphlab/0001/foo
  // with cd it will have less ".."'s. This is still not pretty.
  command_builder << "cd && aws ";
#else
  std::string cmd = "cmd.exe";
  argv.push_back("/c");
  command_builder << "aws ";
#endif
  for (const auto& x: arglist)
    command_builder << x << " ";

  command_builder << "2>" << child_err_file;
  argv.push_back(command_builder.str());

  logstream(LOG_INFO) << "Running aws command: " << command_builder.str() << std::endl;

  std::string ret;
  process shell_proc;
  shell_proc.popen(cmd, argv, STDOUT_FILENO);
  auto progress_rc = wait_on_child_and_print_progress(shell_proc);
  ret += get_child_error_or_empty(child_err_file);
  delete_temp_file(child_err_file);
  if(!progress_rc)
    log_and_throw("Cancelled by user");

  auto shell_rc = shell_proc.get_return_code();
  if(shell_rc == 0) {
    logstream(LOG_INFO) << "Succeeded with error message: " << ret << std::endl;
    ret.clear();
  }

  return ret;
}


}
}
