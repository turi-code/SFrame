# Include the cython flexible type headers.  These are included here
# for the definitions, but the cython_add_standalone_executable macro
# in the CMakeLists pulls in the cython source files directly, so we
# don't actually depend on these libraries at link time.

from ..cython.cy_flexible_type cimport flexible_type, flex_type_enum, UNDEFINED
from ..cython.cy_flexible_type cimport flexible_type_from_pyobject
from ..cython.cy_flexible_type cimport pyobject_from_flexible_type
from ..cython.cy_callback cimport register_exception

from libcpp.string cimport string
from libcpp.vector cimport vector

from cPickle import loads as py_pickle_loads
from cPickle import dumps as py_pickle_dumps
import sys

cdef extern from "<sframe/spark_unity.cpp>" namespace "graphlab::spark_interface":
    flexible_type (*read_flex_obj)(const string& s)
    void (*write_all_rows)(const sframe& sf, size_t row_start, size_t row_end);

    void write_msg(const char* data, size_t bufferLen)

    int _spark_unity_main(int argc, const char **argv)

cdef extern from "<sframe/sframe.hpp>" namespace "graphlab":

    cdef cppclass sframe:
        string column_name(size_t idx)
        size_t num_columns()

cdef extern from "<sframe/sframe_iterators.hpp>" namespace "graphlab":

    cdef cppclass parallel_sframe_iterator_initializer:
        parallel_sframe_iterator_initializer()
        parallel_sframe_iterator_initializer(sframe data, size_t row_start, size_t row_end)

    cdef cppclass parallel_sframe_iterator:
        parallel_sframe_iterator()
        parallel_sframe_iterator(const parallel_sframe_iterator_initializer& data,
                                 size_t thread_idx, size_t n_threads)

        bint done()
        void inc "operator++"()
        flexible_type value(size_t idx)

################################################################################
# The callback function for the _read_flex_obj.
cdef flexible_type _read_flex_obj(const string& pickle_string):
    try:
        return flexible_type_from_pyobject(py_pickle_loads(pickle_string))
    except Exception as e:
        register_exception(e)
        return flexible_type(UNDEFINED)

# Set the correct function pointer.
read_flex_obj = _read_flex_obj

################################################################################
# Write each line of an sframe to a pickled string (call write_message)

cdef void _write_all_rows(const sframe& sf, size_t row_start, size_t row_end):
    cdef dict d = {}
    cdef long i
    cdef long n_columns
    cdef list col_names
    cdef str pickle_str

    cdef parallel_sframe_iterator_initializer* it_init = NULL
    cdef parallel_sframe_iterator* it = NULL

    try:
        n_columns = sf.num_columns()
        col_names = [sf.column_name(i) for i in range(n_columns)]

        it_init = new parallel_sframe_iterator_initializer(sf, row_start, row_end)
        it = new parallel_sframe_iterator(it_init[0], 0, 1)

        while not it.done():
            for i in range(n_columns):
                d[col_names[i]] = pyobject_from_flexible_type(it.value(i))

            pickle_str = py_pickle_dumps(d, protocol=-1)
            write_msg(pickle_str, len(pickle_str))
            it.inc()

    except Exception as e:
        register_exception(e)
    finally:
        if it_init == NULL:
            del it_init
        if it == NULL:
            del it

write_all_rows = _write_all_rows

################################################################################

cdef call_main():
    cdef list args = sys.argv

    cdef vector[const char*] c_args
    c_args.resize(len(args))
    cdef long i

    for i in range(len(args)):
        c_args[i] = args[i]

    sys.exit(_spark_unity_main(len(args), c_args.data()))


if __name__ == "__main__":
    """
    The main function for calling the spark interface.
    """

    call_main()
