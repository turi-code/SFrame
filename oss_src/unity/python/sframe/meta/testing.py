'''
Created on Nov 5, 2011

@author: sean
'''

import sys
import unittest

py2 = sys.version_info.major < 3
py3 = not py2

py2only = unittest.skipIf(not py2, "Only valid for python 2.x")

py3only = unittest.skipIf(not py3, "Only valid for python 3.x")
