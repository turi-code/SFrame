import gl_lib_load_routines as gllib
import sys
    
if __name__ == "__main__":

    ############################################################
    # Load in the spark cython functions.
    
    cy_spark = gllib.load_gl_module("cython", "cy_spark_interface")

    ############################################################
    # Call the proper pylambda worker main function.

    sys.exit(cy_spark.call_spark_main(sys.argv))
    
