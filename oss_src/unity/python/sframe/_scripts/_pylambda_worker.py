from sframe._scripts import gl_lib_load_routines as gllib      # XXX: installation flavor
import sys
import os
import ctypes
from ctypes import PyDLL, c_char_p, c_int, c_void_p
from os.path import split, abspath, join
from glob import glob
from itertools import chain

if __name__ == "__main__":

    ############################################################
    # Set up the logging
    
    if len(sys.argv) == 1:
        dry_run = True
    else:
        dry_run = False

    if dry_run or os.environ.get("GRAPHLAB_LAMBDA_WORKER_DEBUG_MODE") == "1":
        _write_out = sys.stderr
    else:
        _write_out = sys.stdout

    _write_out_file_name = os.environ.get("GRAPHLAB_LAMBDA_WORKER_LOG_FILE", "")
    _write_out_file = None
    
    def _write_log(s, error = False):
        s = s + "\n"

        if error:
            sys.stderr.write(s)
            sys.stderr.flush()
        else:
            _write_out.write(s)
            _write_out.flush()

        if _write_out_file is not None:
            _write_out_file.write(s)
            _write_out_file.flush()

    if _write_out_file_name != "":

        # Set this to an absolute location to make things worthwhile
        _write_out_file_name = abspath(_write_out_file_name)
        os.environ["GRAPHLAB_LAMBDA_WORKER_LOG_FILE"] = _write_out_file_name

        _write_out_file_name = _write_out_file_name + "-init"
        _write_log("Logging initialization routines to %s." % _write_out_file_name)
        try:
            _write_out_file = open(_write_out_file_name, "w")
        except Exception as e:
            _write_log("Error opening '%s' for write: %s" % (_write_out_file_name, repr(e)))
            _write_out_file = None

    _write_log("Lambda worker args: \n  %s" % ("\n  ".join(sys.argv)))
            
    if dry_run:
        print("PyLambda script called with no IPC information; entering diagnostic mode.")

    ############################################################
    # Load the correct pylambda worker library.

    pylambda_lib = gllib.load_internal_ctypes_library(
        "libpylambda_worker*",
        info_log_function = _write_log,
        error_log_function = lambda s: _write_log(s, error=True))

    ############################################################
    # Load in the cython lambda workers.  On import, this will resolve
    # the proper symbols.
    
    if gllib.get_installation_flavor() == "sframe":
        import sframe.cython.cy_pylambda_workers
    else:
        import graphlab.cython.cy_pylambda_workers
    
    ############################################################
    # Call the proper pylambda worker main function.
    
    try:
        pylambda_lib.pylambda_worker_main.argtypes = [c_char_p, c_char_p]
        pylambda_lib.pylambda_worker_main.restype = c_int
    except Exception as e:
        _write_log("Error accessing pylambda_worker_main: %s\n" % repr(e), error = True)
        sys.exit(205)

    main_dir = gllib.get_main_dir()

    if not dry_run:
        # This call only returns after the parent process is done.
        result = pylambda_lib.pylambda_worker_main(c_char_p(main_dir.encode()), c_char_p(sys.argv[1].encode()))
    else:
        # This version will print out a bunch of diagnostic information and then exit.
        result = pylambda_lib.pylambda_worker_main(c_char_p(main_dir.encode()), c_char_p(b"debug"))

    _write_log("Lambda process exited with code %d." % result)
    sys.exit(0)
