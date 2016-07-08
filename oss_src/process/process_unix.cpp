/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <process/process.hpp>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <signal.h>
#include <climits>
#include <mutex>
#include <set>
#include <logger/logger.hpp>
#include <boost/filesystem.hpp>
#include <parallel/mutex.hpp>

namespace fs = boost::filesystem;

/*
 * We need to handle SIGCHLD here to support reaping of processes
 * marked for auto reaping. Basically, all we need to do really is to
 * loop through a set of procids registered by process::autoreap()
 * and call waitpid on them.
 *
 * The trick however is how to make this reentrant safe.
 *
 * To do so, the autoreap function must unregister the signal handler,
 * add the pid to the list of PIDs to reap, and then re-register the signal 
 * handler.
 */
graphlab::mutex sigchld_handler_lock;

// This is an intentional leak of raw pointer because it is used on program termination.
static std::set<size_t>* __proc_ids_to_reap;
static std::once_flag __proc_ids_to_reap_initialized;
static std::set<size_t>& get_proc_ids_to_reap() { 
  std::call_once(__proc_ids_to_reap_initialized, 
                 []() {
                  __proc_ids_to_reap = new std::set<size_t>(); 
                 });
  return *__proc_ids_to_reap;
}

static void sigchld_handler(int sig) {
  // loop through the set of stuff to reap
  // and try to reap them
  auto iter = __proc_ids_to_reap->begin();
  while (iter != __proc_ids_to_reap->end()) {
    if (waitpid(*iter, NULL, WNOHANG) > 0) {
      iter = __proc_ids_to_reap->erase(iter);
    } else {
      ++iter;
    }
  }
}
 
/**
 * Install the SIGCHLD handler. Should be called
 * with sigchld_handler_lock_acquired
 */
static void install_sigchld_handler() {
  struct sigaction act;
  memset (&act, 0, sizeof(act));
  act.sa_handler = sigchld_handler;
  sigaction(SIGCHLD, &act, 0);
}

/**
 * Uninstalls the SIGCHLD handler. Should be called
 * with sigchld_handler_lock_acquired
 */
static void uninstall_sigchld_handler() {
  struct sigaction act;
  memset (&act, 0, sizeof(act));
  act.sa_handler = SIG_DFL;
  sigaction(SIGCHLD, &act, 0);
}

namespace graphlab
{

static const char** convert_args(std::string &cmd, const std::vector<std::string> &args) {
  // Convert args to c_str list
  // Size of filename of cmd + args + NULL
  const char** c_arglist = new const char*[args.size() + 2];
  size_t i = 0;
  c_arglist[i++] = cmd.c_str();
  for (const auto& arg: args) {
    c_arglist[i++] = arg.c_str();
  }
  c_arglist[i] = NULL;

  // std::cerr << "stuff: " << cmd << std::endl;
  /*
   * for(size_t i = 0; c_arglist[i] != NULL; ++i) {
   *   std::cerr << c_arglist[i] << std::endl;
   * }
   */

  return c_arglist;
}

bool process::popen(const std::string &cmd,
                    const std::vector<std::string> &args,
                    int child_write_fd) {
  // build pipe
  int fd[2];
  int& read_fd = fd[0];
  int& write_fd = fd[1];
  if (pipe(fd)) {
    logstream(LOG_ERROR) << "Error building pipe for process launch: " <<
        get_last_err_str(errno);
  }
  std::string cmd_tmp = cmd;
  const char **c_arglist = convert_args(cmd_tmp, args);

#ifdef __APPLE__
  pid_t pid = fork();
#else
  pid_t pid = vfork();
#endif

  if(pid < 0) {
    logstream(LOG_ERROR) << "Fail to fork process: " << strerror(errno) << std::endl;
    delete[] c_arglist;
    return false;
  } else if(pid == 0) {
    //***In child***
    close(read_fd);
    if((child_write_fd > -1) && (write_fd != child_write_fd)) {
      errno = 0;
      if(dup2(write_fd, child_write_fd) != child_write_fd) {
        _exit(1);
      }
      close(write_fd);
    }

    int exec_ret = execvp(&cmd[0], (char**)c_arglist);
    if(exec_ret == -1) {
      std::cerr << "Fail to exec: " << strerror(errno) << std::endl;
    }
    _exit(0);
  } else {
    m_launched = true;
    m_launched_with_popen = true;
    m_pid = pid;
    if(child_write_fd > -1) {
      m_read_handle = read_fd;
    } else {
      close(read_fd);
    }
    close(write_fd);
    delete[] c_arglist;
  }

  logstream(LOG_INFO) << "Launched process with pid: " << m_pid << std::endl;

  return true;
}

/**
 * A "generic" process launcher
 */
bool process::launch(const std::string &cmd,
                             const std::vector<std::string> &args) {
  std::string cmd_tmp = cmd;
  const char **c_arglist = convert_args(cmd_tmp, args);
#ifdef __APPLE__
  pid_t pid = fork();
#else
  pid_t pid = vfork();
#endif

  if(pid < 0) {
    logstream(LOG_ERROR) << "Fail to fork process: " << strerror(errno) << std::endl;
    delete[] c_arglist;
    return false;
  } else if(pid == 0) {
    int exec_ret = execvp(&cmd[0], (char**)c_arglist);
    if(exec_ret == -1) {
      std::cerr << "Fail to exec: " << strerror(errno) << std::endl;
    }
    _exit(0);
  } else {
    m_launched = true;
    m_pid = pid;
    delete[] c_arglist;
  }

  logstream(LOG_INFO) << "Launched process with pid: " << m_pid << std::endl;

  return true;
}

ssize_t process::read_from_child(void *buf, size_t count) {
  if(!m_launched)
    log_and_throw("No process launched!");
  if(!m_launched_with_popen)
    log_and_throw("Cannot read from process launched without a pipe!");
  if(m_read_handle == -1)
    log_and_throw("Cannot read from child, no pipe initialized. "
        "Specify child_write_fd on launch to do this.");
  return read(m_read_handle, buf, count);
}

bool process::kill(bool async) {
  if(!m_launched)
    log_and_throw("No process launched!");

  ::kill(m_pid, SIGKILL);

  if(!async) {
    pid_t wp_rc = waitpid(m_pid, NULL, 0);
    if(wp_rc == -1) {
      auto err_str = get_last_err_str(errno);
      logstream(LOG_INFO) << "Cannot kill process: " << err_str << std::endl;
      return false;
    }
  }

  return true;
}

bool process::exists() {
  if(!m_launched)
    log_and_throw("No process launched!");
  int status;
  auto wp_ret = waitpid(m_pid, &status, WNOHANG);
  if(wp_ret == -1) {
    logstream(LOG_WARNING) << "Failed while checking for existence of process "
      << m_pid << ": " << strerror(errno) << std::endl;
  } else if(wp_ret == 0) {
    return true;
  }

  return false;
}

int process::get_return_code() {
  int status;
  auto wp_ret = waitpid(m_pid, &status, WNOHANG);
  if(wp_ret == -1) {
    return INT_MAX;
  } else if(wp_ret == 0) {
    return INT_MIN;
  } else if(wp_ret != m_pid) {
    return INT_MAX;
  }

  return WEXITSTATUS(status);
}

void process::close_read_pipe() {
  if(!m_launched)
    log_and_throw("No process launched!");
  if(!m_launched_with_popen)
    log_and_throw("Cannot close pipe from process when launched without a pipe!");
  if(m_read_handle == -1)
    log_and_throw("Cannot close pipe from child, no pipe initialized.");
  if(m_read_handle > -1) {
    close(m_read_handle);
    m_read_handle = -1;
  }
}

size_t process::get_pid() {
  return size_t(m_pid);
}

process::~process() {
  if(m_read_handle > -1)
    close(m_read_handle);
}

void process::autoreap() {
  if (m_pid) {
    std::lock_guard<graphlab::mutex> guard(sigchld_handler_lock);
    uninstall_sigchld_handler();
    auto& proc_ids_to_reap = get_proc_ids_to_reap();
    proc_ids_to_reap.insert(m_pid);
    install_sigchld_handler();
  }
}

} // namespace graphlab
