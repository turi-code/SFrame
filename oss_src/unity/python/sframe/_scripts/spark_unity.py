import gl_lib_load_routines as gllib
import sys
    
if __name__ == "__main__":

    ############################################################
    # Load in the spark cython functions.

    if gllib.get_installation_flavor() == "sframe":
        import sframe.cython.cy_spark_interface as cy_spark
    else:
        import graphlab.cython.cy_spark_interface as cy_spark

    ############################################################
    # Call the proper pylambda worker main function.

    sys.exit(cy_spark.call_spark_main(sys.argv))
    
