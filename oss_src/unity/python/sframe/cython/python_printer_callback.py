'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the LICENSE file for details.
'''
import sys
try:
    import IPython
    from IPython.core.interactiveshell import InteractiveShell
    have_ipython = True
except ImportError:
    have_ipython = False

def print_callback(val):
    """
    Internal function.
    This function is called via a call back returning from IPC to Cython
    to Python. It tries to perform incremental printing to IPython Notebook and
    when all else fails, just prints locally.
    """
    success = False
    try:
        # for reasons I cannot fathom, regular printing, even directly
        # to io.stdout does not work.
        # I have to intrude rather deep into IPython to make it behave
        if have_ipython:
            if InteractiveShell.initialized():
                IPython.display.publish_display_data('graphlab.callback', {'text/plain':val,'text/html':'<pre>' + val + '</pre>'})
                success = True
    except:
        pass

    if not success:
        print val
        sys.stdout.flush()
