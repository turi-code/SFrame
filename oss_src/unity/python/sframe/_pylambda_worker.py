import sys
import os
from ctypes import PyDLL, c_char_p, c_int
from os.path import split, abspath, join
from glob import glob
from itertools import chain

if __name__ == "__main__":

    if len(sys.argv) == 1:
        debug_mode = True
    else:
        debug_mode = False

    if debug_mode:
        print "PyLambda script called with no IPC information; entering diagnostic mode."

    script_path = abspath(sys.modules[__name__].__file__)
    main_dir = split(script_path)[0]

    if debug_mode:
        print "Script directory: %s." % script_path
        print "Main program directory: %s." % main_dir
    else:
        print "INFO: launching pylamda_worker in directory %s." % main_dir
        
    # Handle the different library type extensions
    pylambda_workers = glob(join(main_dir, "libpylambda_worker.*"))

    if debug_mode:

        error = False
        
        print ("Found %d candidade pylambda_worker file(s): \n   %s."
               % (len(pylambda_workers), "\n   ".join(pylambda_workers)))

        if len(pylambda_workers) > 1:
            print "WARNING: multiple pylambda worker libraries."
            error = True

        if len(pylambda_workers) == 0:
            print "ERROR: Cannot find pylambda_worker extension library."
            error = True

        print "Installation Directory Structure: "

        visited_files = []

        def on_error(err):
            visited_files.append( ("  ERROR", str(err)) )

        for path, dirs, files in os.walk(main_dir, onerror = on_error):
            for fn in chain(files, dirs):
                name = join(path, fn)
                try:
                    visited_files.append( (name, repr(os.lstat(name))) )
                except:
                    visited_files.append( (name, "ERROR calling os.lstat.") )

        def strip_name(n):
            if n[:len(main_dir)] == main_dir:
                return "<root>/" + n[len(main_dir):]
            else:
                return n
                
        print "\n".join( ("  %s: %s" % (strip_name(name), stats))
                         for name, stats in sorted(visited_files))

        print "INFO: Loading pylambda worker library: ", pylambda_workers[0]
        
    
    pylambda_lib = PyDLL(pylambda_workers[0])

    pylambda_lib.pylambda_worker_main.argtypes = [c_char_p, c_char_p]
    pylambda_lib.pylambda_worker_main.restype = c_int
    
    if not debug_mode:
        # This call only returns after the parent process is done.
        result = pylambda_lib.pylambda_worker_main(c_char_p(main_dir), c_char_p(sys.argv[1]))
    else:
        # This version will print out a bunch of diagnostic information and then exit. 
        result = pylambda_lib.pylambda_worker_main(c_char_p(main_dir), c_char_p("debug"))

    if debug_mode:
        print "Process exited with code %d." % result
