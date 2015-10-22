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
import glob as _glob
import subprocess as _subprocess
import _pylambda_worker

def make_unity_server_env():
    """
    Returns the environemnt for unity_server.

    The environment is necessary to start the unity_server
    by setting the proper enviornments for shared libraries,
    hadoop classpath, and module search paths for python lambda workers.

    The environment has 3 components:
    1. CLASSPATH, contains hadoop class path
    2. LD_LIBRARY_PATH: contains path to shared libraries of jvm and libpython2.7. This is essential for unity_server to get started.
    3. __GL_SYS_PATH__: contains the python sys.path of the interpreter. This is essential for pylambda worker to behave properly.
    """
    env = os.environ.copy()

    # Add hadoop class path
    classpath = get_hadoop_class_path()
    if ("CLASSPATH" in env):
        env["CLASSPATH"] = env['CLASSPATH'] + (os.path.pathsep + classpath if classpath != '' else '')
    else:
        env["CLASSPATH"] = classpath

    # Add JVM path
    libjvm_path = get_libjvm_path()

    # Add libpython path
    libpython_path = get_libpython_path()
    libpath_varname = None
    if sys.platform == 'win32':
        libpath_varname = 'PATH'
    else:
        libpath_varname = 'LD_LIBRARY_PATH'
    if libpath_varname in env:
        env[libpath_varname] = env[libpath_varname] + os.path.pathsep + libpython_path + (os.path.pathsep + libjvm_path if libjvm_path != '' else '')
    else:
        env[libpath_varname] = libpython_path + os.path.pathsep + libjvm_path

    # Add python syspath
    env['__GL_SYS_PATH__'] = (os.path.pathsep).join(sys.path)

    # Add the python executable to the runtime config
    env['__GL_PYTHON_EXECUTABLE__'] = os.path.abspath(sys.executable)

    # Add the pylambda execution script to the runtime config
    env['__GL_PYLAMBDA_SCRIPT__'] = os.path.abspath(_pylambda_worker.__file__)
    
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
    if 'GRAPHLAB_FILEIO_ALTERNATIVE_SSL_CERT_FILE' not in env:
        try:
            import certifi
            env['GRAPHLAB_FILEIO_ALTERNATIVE_SSL_CERT_FILE'] = certifi.where()
        except:
            pass
    return env

def test_pylambda_worker():
    """
    Tests the pylambda workers by spawning off a seperate python
    process in order to print out additional diagnostic information
    in case there is an error.
    """

    import subprocess

    print "\nLaunch pylambda_worker process with simulated unity_server environment."

    proc = subprocess.Popen(
        [sys.executable, os.path.abspath(_pylambda_worker.__file__)],
        env=make_unity_server_env())

    proc.wait()

def get_libpython_path():
    """
    Return the path of libpython2.7.so in the current python environment.

    The path is a guess based on the heuristics that most of the time the library
    is under sys.exec_prefix + "/lib".
    """
    libpython_path = os.path.join(sys.exec_prefix, "lib")
    return libpython_path


__jvm_lib_warned = False


def get_libjvm_path():
    """
    Return path of libjvm.so for LD_LIBRARY_PATH variable.
    This is only required for Linux platform, and by looking in a bunch of 'known' paths.
    """
    cur_platform = _get_platform()
    path_prefixes = []
    path_suffixes = []
    jvm_lib_name = ''

    if cur_platform.startswith("macosx"):
        return ''
    else:
        if sys.platform == 'win32':
            path_suffixes = ['/jre/bin/server','/bin/server']
            jvm_lib_name = 'jvm.dll'
        else:
            path_suffixes = ['/jre/lib/amd64/server']
            path_prefixes = [
                    '/usr/lib/jvm/default-java',               # ubuntu / debian distros
                    '/usr/lib/jvm/java',                       # rhel6
                    '/usr/lib/jvm',                            # centos6
                    '/usr/lib64/jvm',                          # opensuse 13
                    '/usr/local/lib/jvm/default-java',         # alt ubuntu / debian distros
                    '/usr/local/lib/jvm/java',                 # alt rhel6
                    '/usr/local/lib/jvm',                      # alt centos6
                    '/usr/local/lib64/jvm',                    # alt opensuse 13
                    '/usr/local/lib/jvm/java-7-openjdk-amd64', # alt ubuntu / debian distros
                    '/usr/lib/jvm/java-7-openjdk-amd64',       # alt ubuntu / debian distros
                    '/usr/local/lib/jvm/java-6-openjdk-amd64', # alt ubuntu / debian distros
                    '/usr/lib/jvm/java-6-openjdk-amd64',       # alt ubuntu / debian distros
                    '/usr/lib/jvm/java-7-oracle',              # alt ubuntu
                    '/usr/lib/jvm/java-8-oracle',              # alt ubuntu
                    '/usr/lib/jvm/java-6-oracle',              # alt ubuntu
                    '/usr/local/lib/jvm/java-7-oracle',        # alt ubuntu
                    '/usr/local/lib/jvm/java-8-oracle',        # alt ubuntu
                    '/usr/local/lib/jvm/java-6-oracle',        # alt ubuntu
                    '/usr/lib/jvm/default',                    # alt centos
                    '/usr/java/latest',                        # alt centos
                    ]
            jvm_lib_name = 'libjvm.so'

        # First check GRAPHLAB_LIBJVM_DIRECTORY which is expected to be a
        # directory that libjvm.so resides (the path_suffix will not be used)
        # Then, check GRAPHLAB_JAVA_HOME, which will override JAVA_HOME.
        potential_paths = []
        if os.environ.has_key('JAVA_HOME'):
            path_prefixes.insert(0, os.environ['JAVA_HOME'])
        if os.environ.has_key('GRAPHLAB_JAVA_HOME'):
            path_prefixes.insert(0, os.environ['GRAPHLAB_JAVA_HOME'])

        # Construct the full paths to search
        for i in path_suffixes:
            for j in path_prefixes:
                potential_paths.append(j + i)

        if os.environ.has_key('GRAPHLAB_LIBJVM_DIRECTORY'):
            potential_paths.insert(0, os.environ['GRAPHLAB_LIBJVM_DIRECTORY'])
        for path in potential_paths:
            if sys.platform == 'win32':
                path = path.replace('/','\\')
            file_to_try = os.path.join(path, jvm_lib_name)
            if os.path.isfile(file_to_try):
                logging.getLogger(__name__).debug('%s path used: %s' % (jvm_lib_name, path))
                return path

        global __jvm_lib_warned
        if not __jvm_lib_warned:
            __jvm_lib_warned = True
            logging.getLogger(__name__).debug(jvm_lib_name + ' not found. Operations '
            'using HDFS may not work.\nSet GRAPHLAB_JAVA_HOME environment variable'
            ' before starting GraphLab Create to specify preferred java '
            'installation.')
        return ''


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
