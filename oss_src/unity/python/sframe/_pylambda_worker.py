import sys
import os
import ctypes
from ctypes import PyDLL, c_char_p, c_int
from os.path import split, abspath, join
from glob import glob
from itertools import chain

if __name__ == "__main__":

    if len(sys.argv) == 1:
        debug_mode = True
    else:
        debug_mode = False

    if debug_mode or os.environ.get("GRAPHLAB_LAMBDA_WORKER_DEBUG_MODE") == "1":
        write_out = sys.stderr
    else:
        write_out = sys.stdout

    if debug_mode:
        print "PyLambda script called with no IPC information; entering diagnostic mode."

    script_path = abspath(sys.modules[__name__].__file__)
    main_dir = split(script_path)[0]

    write_out.write("Script directory: %s.\n" % script_path)
    write_out.write("Main program directory: %s.\n" % main_dir)
    write_out.flush()

    for s in sys.argv:
        write_out.write("Lambda worker args: \n  %s\n" % ("\n  ".join(sys.argv)))
        write_out.flush()

    # Handle the different library type extensions
    pylamda_worker_search_string = join(main_dir, "libpylambda_worker.*")
    write_out.write("Lambda worker search pattern: %s\n" % pylamda_worker_search_string)
    pylambda_workers = glob(join(main_dir, "libpylambda_worker.*"))

    write_out.write("Found %d candidade pylambda_worker file(s): \n   %s.\n"
                    % (len(pylambda_workers), "\n   ".join(pylambda_workers)))
    write_out.flush()

    if len(pylambda_workers) > 1:
        write_out.write("WARNING: multiple pylambda worker libraries.\n")
        write_out.flush()
        
    if len(pylambda_workers) == 0:
        sys.stderr.write("ERROR: Cannot find pylambda_worker extension library.\n")
        sys.stderr.flush()
        sys.exit(202)

    write_out.write("INFO: Loading pylambda worker library: %s. \n" % pylambda_workers[0])
    write_out.flush()

    try:
        pylambda_lib = PyDLL(pylambda_workers[0])
    except Exception, e:
        sys.stderr.write("Error loading lambda library %s: %s\n" % (pylambda_workers[0], repr(e)))
        sys.stderr.flush()
        sys.exit(203)

    try:
        pylambda_lib.pylambda_worker_main.argtypes = [c_char_p, c_char_p]
        pylambda_lib.pylambda_worker_main.restype = c_int
    except Exception, e:
        sys.stderr.write("Error accessing pylambda_worker_main: %s\n" % repr(e))
        sys.stderr.flush()
        sys.exit(204)
    
    if not debug_mode:
        # This call only returns after the parent process is done.
        result = pylambda_lib.pylambda_worker_main(c_char_p(main_dir), c_char_p(sys.argv[1]))
    else:
        # This version will print out a bunch of diagnostic information and then exit. 
        result = pylambda_lib.pylambda_worker_main(c_char_p(main_dir), c_char_p("debug"))

    write_out.write("Lambda process exited with code %d.\n" % result)
    write_out.flush()
    sys.exit(0)
