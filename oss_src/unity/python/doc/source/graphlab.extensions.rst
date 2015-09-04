:mod:`extensions` 
=========================

This module hosts all the extension functions and classes created via SDK.

The function :py:func:`graphlab.extensions.ext_import` is used to import
a toolkit module (shared library) into the workspace. The shared library can be
directly imported from a remote source, e.g. http, s3, or hdfs.  The imported
module will be under namespace **graphlab.extensions**.

Alternatively, if the shared library is local, it can be directly imported
using the python import statement. Note that graphlab must be imported first.


.. currentmodule:: graphlab.extensions
.. autofunction:: graphlab.extensions.ext_import

