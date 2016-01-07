import sys
import os
import ctypes
from ctypes import PyDLL, c_char_p, c_int, c_void_p
from os.path import split, abspath, join
from glob import glob
from itertools import chain

def set_windows_dll_path():
    """
    Sets the dll load path so that things are resolved correctly.
    """

    # Back up to the directory, then to the base directory as this is
    # in ./_scripts.
    lib_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    def errcheck_bool(result, func, args):
        if not result:
            last_error = ctypes.get_last_error()
            if last_error != 0:
                raise ctypes.WinError(last_error)
            else:
                raise OSError
        return args

    import ctypes.wintypes as wintypes

    # Also need to set the dll loading directory to the main
    # folder so windows attempts to load all DLLs from this
    # directory.
    try:
        kernel32 = ctypes.WinDLL('kernel32', use_last_error=True)
        kernel32.SetDllDirectoryW.errcheck = errcheck_bool
        kernel32.SetDllDirectoryW.argtypes = (wintypes.LPCWSTR,)
        kernel32.SetDllDirectoryW(lib_path)
    except Exception, e:
        sys.stderr.write("Error setting DLL load orders: %s (things may still work).\n" % str(e))
        sys.stderr.flush()

if __name__ == "__main__":

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
        except Exception, e:
            _write_log("Error opening '%s' for write: %s" % (_write_out_file_name, repr(e)))
            _write_out_file = None

    if dry_run:
        print "PyLambda script called with no IPC information; entering diagnostic mode."

    ########################################
    # Set up the system path. 
    
    system_path = os.environ.get("__GL_SYS_PATH__", "")

    del sys.path[:]
    sys.path.extend(p.strip() for p in system_path.split(os.pathsep) if p.strip())

    for i, p in enumerate(sys.path):
        _write_log("  sys.path[%d] = %s. " % (i, sys.path[i]))

    ########################################
    # Now, import thnigs

    script_path = abspath(sys.modules[__name__].__file__)
    main_dir = split(split(script_path)[0])[0]

    _write_log("Script directory: %s." % script_path)
    _write_log("Main program directory: %s." % main_dir)

    for s in sys.argv:
        _write_log("Lambda worker args: \n  %s" % ("\n  ".join(sys.argv)))

    ########################################
    # Set the dll load path if we are on windows
    if sys.platform == 'win32':
        set_windows_dll_path()

    ########################################
    # Find the correct main worker library. 
    pylamda_worker_search_string = join(main_dir, "libpylambda_worker.*")
    _write_log("Lambda worker search pattern: %s\n" % pylamda_worker_search_string)
    pylambda_workers = glob(join(main_dir, "libpylambda_worker.*"))

    _write_log("Found %d candidade pylambda_worker file(s): \n   %s."
               % (len(pylambda_workers), "\n   ".join(pylambda_workers)))

    if len(pylambda_workers) > 1:
        _write_log("WARNING: multiple pylambda worker libraries.")

    if len(pylambda_workers) == 0:
        _write_log("ERROR: Cannot find pylambda_worker extension library.", error = True)
        sys.exit(202)

    _write_log("INFO: Loading pylambda worker library: %s." % pylambda_workers[0])

    module_name = os.path.split(main_dir)[1]

    if module_name != "graphlab" and module_name != "sframe":
        _write_log("Module path not sframe or graphlab.", error = True)
        sys.exit(200)

    ########################################
    # Get the pointers to the cython callback functions that can
    # actually execute the lambda functions.

    try:
        pylambda_lib = PyDLL(pylambda_workers[0], mode=ctypes.RTLD_GLOBAL)
    except Exception, e:
        _write_log("Error loading lambda library %s: %s" % (pylambda_workers[0], repr(e)), error = True)
        sys.exit(203)

    # Load in the cython lambda workers.  On import, this will resolve
    # the proper symbols.
    if module_name == "graphlab":
        import graphlab.cython.cy_pylambda_workers
    else:
        import sframe.cython.cy_pylambda_workers

    try:
        pylambda_lib.pylambda_worker_main.argtypes = [c_char_p, c_char_p]
        pylambda_lib.pylambda_worker_main.restype = c_int
    except Exception, e:
        _write_log("Error accessing pylambda_worker_main: %s\n" % repr(e), error = True)
        sys.exit(205)

    if not dry_run:
        # This call only returns after the parent process is done.
        result = pylambda_lib.pylambda_worker_main(c_char_p(main_dir), c_char_p(sys.argv[1]))
    else:
        # This version will print out a bunch of diagnostic information and then exit.
        result = pylambda_lib.pylambda_worker_main(c_char_p(main_dir), c_char_p("debug"))

    _write_log("Lambda process exited with code %d." % result)
    sys.exit(0)
