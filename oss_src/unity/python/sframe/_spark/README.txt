The cython modules needed by the pyspark_unity.py script must be
relatively self-contained; i.e. copying all the liblraries compiled in
this directory into a single other directory should be enough for the
pyspark_unity.py to run on its own.

DO NOT put a __init__.py file in this directory.  If you do that, then
cython inserts absolute imports within the whole package, and we can't
have these things be isolated.  

