'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''
from .. import extensions, set_runtime_config, get_runtime_config
from .. import connect as _mt

def connect_odbc(conn_str):
    """
    Create a stateful connection with a database.

    An ODBC driver manager program (unixODBC) must be installed with one or
    more functional drivers in order to use this feature.  Please see the `User Guide 
    <https://dato.com/learn/userguide/data_formats_and_sources/odbc_integration.html>`_
    for more details.

    Parameters
    ----------
    conn_str : str
        A standard ODBC connection string.

    Returns
    -------
    out : graphlab.extensions._odbc_connection.unity_odbc_connection

    Examples
    --------
    >>> db = graphlab.connect_odbc("DSN=my_awesome_dsn;UID=user;PWD=mypassword")
    """
    db = extensions._odbc_connection.unity_odbc_connection()
    db._construct_from_odbc_conn_str(conn_str)
    _mt._get_metric_tracker().track('connect_odbc', properties={'dbms_name':db.dbms_name,'dbms_version':db.dbms_version})
    return db

def set_libodbc_path(path):
    """
    Set the first path that GraphLab Create will search for libodbc.so.

    Since ODBC requires a driver manager to be installed system-wide, we
    provide this to help you if it is installed in a non-standard location.
    GraphLab Create will also search on the system's default library paths, so
    if you installed your driver manager in a standard way, you shouldn't need
    to worry about this function.
    """
    set_runtime_config('GRAPHLAB_LIBODBC_PREFIX', path)

def get_libodbc_path():
    """
    Get the first path that GraphLab Create will search for libodbc.so.
    """
    c = get_runtime_config()
    return c['GRAPHLAB_LIBODBC_PREFIX']
