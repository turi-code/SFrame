'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''
import sys
import os
import logging
from distutils.util import get_platform as _get_platform
import ctypes
import glob as _glob
import subprocess as _subprocess
import _scripts._pylambda_worker as _pylambda_worker
from copy import copy


def make_unity_server_env():
    """
    Returns the environemnt for unity_server.

    The environment is necessary to start the unity_server
    by setting the proper enviornments for shared libraries,
    hadoop classpath, and module search paths for python lambda workers.

    The environment has 3 components:
    1. CLASSPATH, contains hadoop class path
    2. __GL_PYTHON_EXECUTABLE__, path to the python executable
    3. __GL_PYLAMBDA_SCRIPT__, path to the lambda worker executable
    4. __GL_SYS_PATH__: contains the python sys.path of the interpreter
    """
    env = os.environ.copy()

    # Add hadoop class path
    classpath = get_hadoop_class_path()
    if ("CLASSPATH" in env):
        env["CLASSPATH"] = env['CLASSPATH'] + (os.path.pathsep + classpath if classpath != '' else '')
    else:
        env["CLASSPATH"] = classpath

    # Add python syspath
    env['__GL_SYS_PATH__'] = (os.path.pathsep).join(sys.path)

    # Add the python executable to the runtime config
    env['__GL_PYTHON_EXECUTABLE__'] = os.path.abspath(sys.executable)

    # Add the pylambda execution script to the runtime config
    env['__GL_PYLAMBDA_SCRIPT__'] = os.path.abspath(_pylambda_worker.__file__)

    # For Windows, add path to DLLs for the pylambda_worker
    if sys.platform == 'win32':
        set_windows_dll_path()

    #### Remove PYTHONEXECUTABLE ####
    # Anaconda overwrites this environment variable
    # which forces all python sub-processes to use the same binary.
    # When using virtualenv with ipython (which is outside virtualenv),
    # all subprocess launched under unity_server will use the
    # conda binary outside of virtualenv, which lacks the access
    # to all packeages installed inside virtualenv.
    if 'PYTHONEXECUTABLE' in env:
        del env['PYTHONEXECUTABLE']

    ## set local to be c standard so that unity_server will run ##
    env['LC_ALL']='C'
    # add certificate file
    if 'GRAPHLAB_FILEIO_ALTERNATIVE_SSL_CERT_FILE' not in env and \
            'GRAPHLAB_FILEIO_ALTERNATIVE_SSL_CERT_DIR' not in env:
        try:
            import certifi
            env['GRAPHLAB_FILEIO_ALTERNATIVE_SSL_CERT_FILE'] = certifi.where()
            env['GRAPHLAB_FILEIO_ALTERNATIVE_SSL_CERT_DIR'] = ""
        except:
            pass
    return env

def set_windows_dll_path():
    """
    Sets the dll load path so that things are resolved correctly.
    """

    lib_path = os.path.dirname(os.path.abspath(_pylambda_worker.__file__))

    def errcheck_bool(result, func, args):
        if not result:
            last_error = ctypes.get_last_error()
            if last_error != 0:
                raise ctypes.WinError(last_error)
            else:
                raise OSError
        return args

    # Also need to set the dll loading directory to the main
    # folder so windows attempts to load all DLLs from this
    # directory.
    import ctypes.wintypes as wintypes

    try:
        kernel32 = ctypes.WinDLL('kernel32', use_last_error=True)
        kernel32.SetDllDirectoryW.errcheck = errcheck_bool
        kernel32.SetDllDirectoryW.argtypes = (wintypes.LPCWSTR,)
        kernel32.SetDllDirectoryW(lib_path)
    except Exception, e:
        logging.getLogger(__name__).warning(
            "Error setting DLL load orders: %s (things should still work)." % str(e))


def test_pylambda_worker():
    """
    Tests the pylambda workers by spawning off a seperate python
    process in order to print out additional diagnostic information
    in case there is an error.
    """

    print "\nLaunch pylambda_worker process with simulated unity_server environment."

    import subprocess

    env=make_unity_server_env()
    env["GRAPHLAB_LAMBDA_WORKER_DEBUG_MODE"] = "1"
    proc = subprocess.Popen(
            [sys.executable, os.path.abspath(_pylambda_worker.__file__)],
            env = env)

    proc.wait()

def dump_directory_structure():
    """
    Dumps a detailed report of the graphlab/sframe directory structure
    and files, along with the output of os.lstat for each.  This is useful
    for debugging purposes.
    """

    "Dumping Installation Directory Structure for Debugging: "

    import sys, os
    from os.path import split, abspath, join
    from itertools import chain
    main_dir = split(abspath(sys.modules[__name__].__file__))[0]

    visited_files = []

    def on_error(err):
        visited_files.append( ("  ERROR", str(err)) )

    for path, dirs, files in os.walk(main_dir, onerror = on_error):
        for fn in chain(files, dirs):
            name = join(path, fn)
            try:
                visited_files.append( (name, repr(os.lstat(name))) )
            except:
                visited_files.append( (name, "ERROR calling os.lstat.") )

    def strip_name(n):
        if n[:len(main_dir)] == main_dir:
            return "<root>/" + n[len(main_dir):]
        else:
            return n

    print "\n".join( ("  %s: %s" % (strip_name(name), stats))
                     for name, stats in sorted(visited_files))

__hadoop_class_warned = False

def get_hadoop_class_path():
    # Try get the classpath directly from executing hadoop
    env = os.environ.copy()
    hadoop_exe_name = 'hadoop'
    if sys.platform == 'win32':
        hadoop_exe_name += '.cmd'
    output = None
    try:
        try:
            output = _subprocess.check_output([hadoop_exe_name, 'classpath'])
        except:
            output = _subprocess.check_output(['/'.join([env['HADOOP_HOME'],'bin',hadoop_exe_name]), 'classpath'])

        output = (os.path.pathsep).join(os.path.realpath(path) for path in output.split(os.path.pathsep))
        return _get_expanded_classpath(output)

    except Exception as e:
        global __hadoop_class_warned
        if not __hadoop_class_warned:
            __hadoop_class_warned = True
            logging.getLogger(__name__).debug("Exception trying to retrieve Hadoop classpath: %s" % e)

    logging.getLogger(__name__).debug("Hadoop not found. HDFS url is not supported. Please make hadoop available from PATH or set the environment variable HADOOP_HOME.")
    return ""


def _get_expanded_classpath(classpath):
    """
    Take a classpath of the form:
      /etc/hadoop/conf:/usr/lib/hadoop/lib/*:/usr/lib/hadoop/.//*: ...

    and return it expanded to all the JARs (and nothing else):
      /etc/hadoop/conf:/usr/lib/hadoop/lib/netty-3.6.2.Final.jar:/usr/lib/hadoop/lib/jaxb-api-2.2.2.jar: ...

    mentioned in the path
    """
    if classpath is None or classpath == '':
        return ''

    #  so this set comprehension takes paths that end with * to be globbed to find the jars, and then
    #  recombined back into a colon separated list of jar paths, removing dupes and using full file paths
    jars = (os.path.pathsep).join((os.path.pathsep).join([os.path.abspath(jarpath) for jarpath in _glob.glob(path)])
                    for path in classpath.split(os.path.pathsep))
    logging.getLogger(__name__).debug('classpath being used: %s' % jars)
    return jars
