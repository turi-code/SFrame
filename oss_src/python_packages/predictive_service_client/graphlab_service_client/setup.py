#!/usr/bin/env python

"""
Copyright (C) 2016 Turi

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import os
import sys
import glob
import subprocess
from setuptools import setup, find_packages

PACKAGE_NAME="graphlab-service-client"
VERSION="1.3.0"

if __name__ == '__main__':
    setup(
        name="GraphLab-Service-Client",
        version=VERSION,
        author='Turi',
        author_email='contact@turi.com',
        packages=find_packages(),
        url='https://turi.com',
        license='LICENSE.txt',
        description='GraphLab Service Client makes it easy to make REST API calls to GraphLab Predictive Services',
        classifiers=[
            "Development Status :: 4 - Beta",
            "Environment :: Console",
            "Intended Audience :: Developers",
            "Intended Audience :: Financial and Insurance Industry",
            "Intended Audience :: Information Technology",
            "Intended Audience :: Other Audience",
            "Intended Audience :: Science/Research",
            "License :: OSI Approved :: BSD License",
            "Natural Language :: English",
            "Operating System :: MacOS :: MacOS X",
            "Operating System :: POSIX :: Linux",
            "Operating System :: POSIX :: BSD",
            "Operating System :: Unix",
            "Programming Language :: Python :: 2.7",
            "Topic :: Scientific/Engineering",
            "Topic :: Internet :: WWW/HTTP :: WSGI :: Application",
            "Topic :: Scientific/Engineering :: Information Analysis",
        ],
        install_requires=[
            "requests == 2.3.0",
        ],
    )
