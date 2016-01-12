'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''
from distutils.version import StrictVersion
import logging
import re


def __get_version(version):
    # matching 1.6.1, and 1.6.1rc, 1.6.1.dev
    version_regex = '^\d+\.\d+\.\d+'
    version = re.search(version_regex, str(version)).group(0)
    return StrictVersion(version)


HAS_PANDAS = True
PANDAS_MIN_VERSION = '0.13.0'
try:
    import pandas
    if __get_version(pandas.__version__) < StrictVersion(PANDAS_MIN_VERSION):
        HAS_PANDAS = False
        logging.warn(('Pandas version %s is not supported. Minimum required version: %s. '
                      'Pandas support will be disabled.')
                      % (pandas.__version__, PANDAS_MIN_VERSION) )
except:
    HAS_PANDAS = False
    from . import pandas_mock as pandas


HAS_NUMPY = True
NUMPY_MIN_VERSION = '1.8.0'
try:
    import numpy

    if __get_version(numpy.__version__) < StrictVersion(NUMPY_MIN_VERSION):
        HAS_NUMPY = False
        logging.warn(('Numpy version %s is not supported. Minimum required version: %s. '
                      'Numpy support will be disabled.')
                      % (numpy.__version__, NUMPY_MIN_VERSION) )
except:
    HAS_NUMPY = False
    from . import numpy_mock as numpy


HAS_SKLEARN = True
SKLEARN_MIN_VERSION = '0.15'
def __get_sklearn_version(version):
    # matching 0.15b, 0.16bf, etc
    version_regex = '^\d+\.\d+'
    version = re.search(version_regex, str(version)).group(0)
    return StrictVersion(version)

try:
    import sklearn
    if __get_sklearn_version(sklearn.__version__) < StrictVersion(SKLEARN_MIN_VERSION):
        HAS_SKLEARN = False
        logging.warn(('sklearn version %s is not supported. Minimum required version: %s. '
                      'sklearn support will be disabled.')
                      % (sklearn.__version__, SKLEARN_MIN_VERSION) )
except:
    HAS_SKLEARN = False
    from . import sklearn_mock as sklearn


HAS_NLTK = True
NLTK_MIN_VERSION = '3.0'
def __get_nltk_version(version):
    version_regex = '^\d+\.\d+'
    version = re.search(version_regex, str(version)).group(0)
    return StrictVersion(version)

try:
    import nltk
    if __get_nltk_version(nltk.__version__) < StrictVersion(NLTK_MIN_VERSION):
        HAS_NLTK = False
        logging.warn(('nltk version %s is not supported. Minimum required version: %s. '
                      'nltk support will be disabled.')
                      % (nltk.__version__, NLTK_MIN_VERSION) )
except:
    HAS_NLTK = False
    from . import nltk_mock as nltk
