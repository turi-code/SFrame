# -*- coding: utf-8 -*-
'''
Copyright (C) 2016 Turi
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''
from __future__ import print_function

from ..sys_util import get_config_file
from ..sys_util import setup_environment_from_config_file
from ..sys_util import write_config_file_value
from ..sys_util import get_library_name
from ..sys_util import make_unity_server_env

import unittest
import tempfile
import subprocess
import sys

from os.path import join
import os
import shutil

class EnvironmentConfigTester(unittest.TestCase):


    def test_config_basic_write(self):

        test_dir = tempfile.mkdtemp()
        config_file = join(test_dir, "test_config")
        os.environ["GRAPHLAB_CONFIG_FILE"] = config_file

        try:
            self.assertEqual(get_config_file(), config_file)
            write_config_file_value("GRAPHLAB_FILE_TEST_VALUE", "this-is-a-test")
            setup_environment_from_config_file()
            self.assertEqual(os.environ["GRAPHLAB_FILE_TEST_VALUE"], "this-is-a-test")
        finally:
            shutil.rmtree(test_dir)
            del os.environ["GRAPHLAB_CONFIG_FILE"]

    # @hoytak -- this is disabled because it fails deterministically on Jenkins
    # (Dato-Dev-Release-Build) and on my MacBook Pro.
    # Please investigate and re-enable when possible.
    @unittest.skip("This doesn't seem to pass at all in dato-dev.")
    def test_environment_import(self):

        test_dir = tempfile.mkdtemp()
        config_file = join(test_dir, "test_config")

        os.environ["GRAPHLAB_CONFIG_FILE"] = config_file
        write_config_file_value("GRAPHLAB_FILEIO_MAXIMUM_CACHE_CAPACITY", "123456")

        run_script = r"""
import sys
import os

system_path = os.environ.get("__GL_SYS_PATH__", "")
del sys.path[:]
sys.path.extend(p.strip() for p in system_path.split(os.pathsep) if p.strip())

import %(library)s

var = %(library)s.get_runtime_config()["GRAPHLAB_FILEIO_MAXIMUM_CACHE_CAPACITY"]

if var == 123456:
    sys.exit(0)
else:
    print("boo: GRAPHLAB_FILEIO_MAXIMUM_CACHE_CAPACITY = ", var, "and this is wrong.  Seriously.")
    sys.exit(1)

""" % {"library" : get_library_name()}

        run_file = join(test_dir, "run_test.py")

        with open(run_file, 'w') as f:
            f.write(run_script)

        env = make_unity_server_env()
        env["GRAPHLAB_CONFIG_FILE"] = config_file

        ret_code = subprocess.call([sys.executable, run_file], env = env)

        self.assertEqual(ret_code, 0)
