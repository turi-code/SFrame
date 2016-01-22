import gl_lib_load_routines as gllib
import sys
import os
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
        _write_out = None

    _write_out_file_name = os.environ.get("GRAPHLAB_LAMBDA_WORKER_LOG_FILE", "")
    _write_out_file = None
    
    def _write_log(s, error = False):
        s = s + "\n"

        if error:
            try:
                sys.stderr.write(s)
                sys.stderr.flush()
            except Exception:
                # Can't do anything in this case, as it can be because
                # of a bad file descriptor passed on from a windows
                # subprocess
                pass
            
        elif _write_out is not None:
            try:
                _write_out.write(s)
                _write_out.flush()
            except Exception:
                pass

        if _write_out_file is not None:
            try:
                _write_out_file.write(s)
                _write_out_file.flush()
            except Exception:
                pass

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

    for s in sys.argv:
        _write_log("Lambda worker args: \n  %s" % ("\n  ".join(sys.argv)))
            
    if dry_run:
        print "PyLambda script called with no IPC information; entering diagnostic mode."

    gllib.setup_environment(info_log_function = _write_log,
                            error_log_function = lambda s: _write_log(s, error=True))
        
    ############################################################
    # Load in the cython lambda workers.  On import, this will resolve
    # the proper symbols.
    
    if gllib.get_installation_flavor() == "sframe":
        from sframe.cython.cy_pylambda_workers import run_pylambda_worker
    else:
        from graphlab.cython.cy_pylambda_workers import run_pylambda_worker
    
    main_dir = gllib.get_main_dir()

    default_loglevel = 5  # 5: LOG_WARNING, 4: LOG_PROGRESS  3: LOG_EMPH  2: LOG_INFO  1: LOG_DEBUG
    dryrun_loglevel = 1  # 5: LOG_WARNING, 4: LOG_PROGRESS  3: LOG_EMPH  2: LOG_INFO  1: LOG_DEBUG
    
    if not dry_run:
        # This call only returns after the parent process is done.
        result = run_pylambda_worker(main_dir, sys.argv[1], default_loglevel)
    else:
        # This version will print out a bunch of diagnostic information and then exit.
        result = run_pylambda_worker(main_dir, "debug", dryrun_loglevel)

    _write_log("Lambda process exited with code %d." % result)
    sys.exit(0)
