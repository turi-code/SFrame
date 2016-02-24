# Include the cython flexible type headers.  These are included here
# for the definitions, but the cython_add_standalone_executable macro
# in the CMakeLists pulls in the cython source files directly, so we
# don't actually depend on these libraries at link time.

from cy_spark_unity import main

if __name__ == "__main__":
    main()
