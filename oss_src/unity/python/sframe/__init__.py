"""
@package graphlab
...
GraphLab Create is a machine learning platform that enables data scientists and
app developers to easily create intelligent applications at scale. Building an
intelligent, predictive application involves iterating over multiple steps:
cleaning the data, developing features, training a model, and creating and
maintaining a predictive service. GraphLab Create does all of this in one
platform. It is easy to use, fast, and powerful.

For more details on the GraphLab Create see http://turi.com, including
documentation, tutorial, etc.
"""

'''
Copyright (C) 2016 Turi
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''

# Important to call this before everything else
from .sys_util import setup_environment_from_config_file \
     as _setup_environment_from_config_file

_setup_environment_from_config_file()


from . import util

from .util import set_runtime_config
from .util import get_runtime_config
from .version_info import __VERSION__, version, build_number

from .connect import _get_metric_tracker
from . import visualization

import os as _os
import sys as _sys
if _sys.platform != 'win32' or \
    (_os.path.exists(_os.path.join(_os.path.dirname(__file__), 'cython', 'libstdc++-6.dll')) and \
    _os.path.exists(_os.path.join(_os.path.dirname(__file__), 'cython', 'libgcc_s_seh-1.dll'))):
    from .data_structures.sgraph import Vertex, Edge
    from .data_structures.sgraph import SGraph
    from .data_structures.sframe import SFrame
    from .data_structures.sarray import SArray
    from .data_structures.sketch import Sketch
    from .data_structures.image import Image
    from .data_structures.sarray_builder import SArrayBuilder
    from .data_structures.sframe_builder import SFrameBuilder

    from .data_structures.sgraph import load_sgraph, load_graph

    from .toolkits._model import Model, CustomModel, load_model

    from . import aggregate
    from . import toolkits

    from .toolkits.image_analysis import image_analysis

    from .data_structures.sframe import load_sframe, get_spark_integration_jar_path
    from .data_structures.DBConnection import connect_odbc, get_libodbc_path, set_libodbc_path

    # internal util
    from .connect.main import launch as _launch
    from .connect.main import stop as _stop
    from .connect import main as glconnect


    from .util import get_environment_config
    from .util import get_graphlab_object_type
    from .util import get_log_location, get_client_log_location, get_server_log_location

    from .version_info import version
    from .version_info import __VERSION__


    class DeprecationHelper(object):
        def __init__(self, new_target):
            self.new_target = new_target

        def _warn(self):
            import warnings
            import logging
            warnings.warn("Graph has been renamed to SGraph. The Graph class will be removed in the next release.", PendingDeprecationWarning)
            logging.warning("Graph has been renamed to SGraph. The Graph class will be removed in the next release.")

        def __call__(self, *args, **kwargs):
            self._warn()
            return self.new_target(*args, **kwargs)

        def __getattr__(self, attr):
            self._warn()
            return getattr(self.new_target, attr)

    Graph = DeprecationHelper(SGraph)

    from .cython import cy_pylambda_workers
    ################### Extension Importing ########################
    from . import extensions
    from .extensions import ext_import

    extensions._add_meta_path()

    # rewrite the extensions module
    class _extensions_wrapper(object):
      def __init__(self, wrapped):
        self._wrapped = wrapped
        self.__doc__ = wrapped.__doc__

      def __getattr__(self, name):
        try:
            return getattr(self._wrapped, name)
        except:
            pass
        from .connect.main import get_unity
        get_unity()
        return getattr(self._wrapped, name)

    from . import _json as json # imports from _json.py in this directory

    _sys.modules[__name__ + ".extensions"] = _extensions_wrapper(_sys.modules[__name__ + ".extensions"])
    # rewrite the import
    extensions = _sys.modules[__name__ + ".extensions"]
else:
    from dependencies import get_dependencies
    package_dir = _os.path.dirname(__file__)
    print("""
ACTION REQUIRED: Dependencies libstdc++-6.dll and libgcc_s_seh-1.dll not found.

1. Ensure user account has write permission to %s
2. Run sframe.get_dependencies() to download and install them.
3. Restart Python and import sframe again.

By running the above function, you agree to the following licenses.

* libstdc++: https://gcc.gnu.org/onlinedocs/libstdc++/manual/license.html
* xz: http://git.tukaani.org/?p=xz.git;a=blob;f=COPYING
  """ % package_dir)
