'''
Copyright (C) 2016 Turi
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''
import os
import time
import logging
import platform
import sys as _sys

class GraphLabConfig:

    __slots__ = ['graphlab_server', 'server_addr', 'server_bin', 'log_dir', 'unity_metric',
                 'mode',
                 'log_rotation_interval','log_rotation_truncate', 'metrics_url']

    def __init__(self, server_addr=None):
        if not server_addr:
            server_addr = 'default'
        self.server_addr = server_addr
        gl_root = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..'))
        self.log_rotation_interval = 86400
        self.log_rotation_truncate = 8
        if "GRAPHLAB_UNITY" in os.environ:
            self.server_bin = os.environ["GRAPHLAB_UNITY"]
        elif os.path.exists(os.path.join(gl_root, "unity_server")):
            self.server_bin = os.path.join(gl_root, "unity_server")
        elif os.path.exists(os.path.join(gl_root, "unity_server.exe")):
            self.server_bin = os.path.join(gl_root, "unity_server.exe")

        if "GRAPHLAB_LOG_ROTATION_INTERVAL" in os.environ:
            tmp = os.environ["GRAPHLAB_LOG_ROTATION_INTERVAL"]
            try:
                self.log_rotation_interval = int(tmp)
            except:
                logging.getLogger(__name__).warning("GRAPHLAB_LOG_ROTATION_INTERVAL must be an integral value")

        if "GRAPHLAB_LOG_ROTATION_TRUNCATE" in os.environ:
            tmp = os.environ["GRAPHLAB_LOG_ROTATION_TRUNCATE"]
            try:
                self.log_rotation_truncate = int(tmp)
            except:
                logging.getLogger(__name__).warning("GRAPHLAB_LOG_ROTATION_TRUNCATE must be an integral value")

        if "GRAPHLAB_LOG_PATH" in os.environ:
            log_dir = os.environ["GRAPHLAB_LOG_PATH"]
        else:
            if platform.system() == "Windows":
                if "TEMP" in os.environ:
                    log_dir = os.environ["TEMP"]
                else:
                    raise "Please set the TEMP environment variable"
            else:
                log_dir = "/tmp"

        self.log_dir = log_dir
        ts = str(int(time.time()))
        root_package_name = __import__(__name__.split('.')[0]).__name__
        self.unity_metric = os.path.join(log_dir, root_package_name + '_metric_' + str(ts) + '.log')

        # Import our unity conf file if it exists, and get the mode from it
        try:
          import graphlab_env
        except ImportError:
          self.graphlab_server = ''
          self.mode = 'UNIT'
          self.metrics_url = ''
        else:
          if graphlab_env.mode in ['UNIT', 'DEV', 'QA', 'PROD']:
            self.mode = graphlab_env.mode
          else:
            self.mode = 'PROD'

          self.graphlab_server = graphlab_env.graphlab_server
          self.metrics_url = graphlab_env.metrics_url

          # NOTE: Remember to update slots if you are adding any config parameters to this file.

    def get_unity_log(self):
        ts = str(int(time.time()))
        log_ext = '.log'
        root_package_name = __import__(__name__.split('.')[0]).__name__
        return os.path.join(self.log_dir, root_package_name + '_server_' + str(ts) + log_ext)

DEFAULT_CONFIG = GraphLabConfig()
