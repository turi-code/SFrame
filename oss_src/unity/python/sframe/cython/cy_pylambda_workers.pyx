#cython: language_level=2, c_string_encoding=utf8

from cy_flexible_type cimport flexible_type, flex_type_enum, UNDEFINED
from cy_flexible_type cimport flexible_type_from_pyobject
from cy_flexible_type cimport process_common_typed_list
from cy_flexible_type cimport pyobject_from_flexible_type
from .._gl_pickle import GLUnpickler
import cPickle as py_pickle
from copy import deepcopy
from libcpp.string cimport string
from libcpp.vector cimport vector
import ctypes
import os
import traceback
import sys
cimport cython

from random import seed as set_random_seed

cdef extern from "<sframe/sframe_rows.hpp>" namespace "graphlab":

    cdef struct row "sframe_rows::row":
        const flexible_type& at "operator[]"(size_t col)
        
    cdef struct sframe_rows:
        row at "operator[]"(size_t row)

        size_t num_columns()
        size_t num_rows()


cdef extern from "<lambda/pylambda.hpp>" namespace "graphlab::lambda":

    cdef struct lambda_exception_info:
        bint exception_occured
        string exception_pickle
        string exception_string

    cdef struct lambda_call_data:
        flex_type_enum output_enum_type
        bint skip_undefined

        # It's the responsibility of the calling class to make sure
        # these are valid.
        const flexible_type* input_values
        flexible_type* output_values
        size_t n_inputs

    cdef struct lambda_call_by_dict_data:
        flex_type_enum output_enum_type

        const vector[string]* input_keys
        const vector[vector[flexible_type] ]* input_rows

        flexible_type* output_values

    cdef struct lambda_call_by_sframe_rows_data:
        flex_type_enum output_enum_type

        const vector[string]* input_keys
        const sframe_rows* input_rows

        flexible_type* output_values
        
    cdef struct pylambda_evaluation_functions:
        void (*set_random_seed)(size_t seed)
        size_t (*init_lambda)(const string&, lambda_exception_info*)
        void (*release_lambda)(size_t, lambda_exception_info*)
        void (*eval_lambda)(size_t, lambda_call_data*, lambda_exception_info*)
        void (*eval_lambda_by_dict)(size_t, lambda_call_by_dict_data*, lambda_exception_info*)
        void (*eval_lambda_by_sframe_rows)(size_t, lambda_call_by_sframe_rows_data*, lambda_exception_info*)
        

        

################################################################################

cdef class lambda_evaluator(object):
    cdef object lambda_function
    cdef list output_buffer
    cdef str lambda_string

    def __init__(self, str lambda_string):

        self.lambda_string = lambda_string

        cdef bint is_directory

        try:
            is_directory = os.path.isdir(self.lambda_string)
        except Exception:
            is_directory = False

        if is_directory:
            unpickler = GLUnpickler(self.lambda_string)
            self.lambda_function = unpickler.load()
        else:
            self.lambda_function = py_pickle.loads(self.lambda_string)

        self.output_buffer = []

    @cython.boundscheck(False)
    cdef eval_simple(self, lambda_call_data* lcd):
        
        cdef const flexible_type* v_in = lcd.input_values
        cdef long n = lcd.n_inputs
        
        if len(self.output_buffer) != n:
            self.output_buffer = [None]*n
            
        cdef long i
        cdef object x
        
        for i in range(0, n):
            if v_in[i].get_type() == UNDEFINED and self.skip_undefined:
                self.output_buffer[i] = None
                continue
            
            x = pyobject_from_flexible_type(v_in[i])
            self.output_buffer[i] = self.lambda_function(x)

        process_common_typed_list(lcd.output_values, self.output_buffer, lcd.output_enum_type)

    @cython.boundscheck(False)
    cdef eval_multiple_rows(self, lambda_call_by_dict_data* lcd):

        cdef dict arg_dict = {}
        cdef long i, j
        cdef long n = lcd.input_rows[0].size()
        cdef long n_keys = lcd.input_keys[0].size()

        cdef list keys = [lcd.input_keys[0][i] for i in range(n_keys)]

        if len(self.output_buffer) != n:
            self.output_buffer = [None]*n

        for i in range(n):

            if lcd.input_keys[0][i].size() != n_keys:
                raise ValueError("Row %d does not have the correct number of rows (%d, should be %d)"
                                 % (lcd.input_keys[0][i].size(), n))

            arg_dict = {}
            
            for j in range(n_keys):
                arg_dict[keys[j]] = pyobject_from_flexible_type(lcd.input_rows[0][i][j])

            self.output_buffer[i] = self.lambda_function(arg_dict)

        process_common_typed_list(lcd.output_values, self.output_buffer, lcd.output_enum_type)

    @cython.boundscheck(False)
    cdef eval_sframe_rows(self, lambda_call_by_sframe_rows_data* lcd):

        cdef dict arg_dict = {}
        cdef long i, j
        cdef long n = lcd.input_rows[0].num_rows()
        cdef long n_keys = lcd.input_keys[0].size()

        assert lcd.input_rows[0].num_columns() == n_keys

        cdef list keys = [lcd.input_keys[0][i] for i in range(n_keys)]

        if len(self.output_buffer) != n:
            self.output_buffer = [None]*n

        for i in range(n):
            arg_dict = {}

            for j in range(n_keys):
                arg_dict[keys[j]] = pyobject_from_flexible_type(lcd.input_rows[0].at(i).at(j))

            self.output_buffer[i] = self.lambda_function(arg_dict)

        process_common_typed_list(lcd.output_values, self.output_buffer, lcd.output_enum_type)
        

################################################################################
# Wrapping functions

cdef object _lambda_counter = 1
cdef dict _lambda_id_to_evaluator = {}
cdef dict _lambda_function_string_to_id = {}

cdef lambda_evaluator _get_lambda_class(size_t lmfunc_id):
    try:
        return <lambda_evaluator>_lambda_id_to_evaluator[lmfunc_id]
    except KeyError:
        raise ValueError("Invalid id for lambda function (%ld)." % lmfunc_id)


cdef pylambda_evaluation_functions eval_functions


cdef void _process_exception(object e, str traceback_str, lambda_exception_info* lei):
    """
    Process any possible exception.
    """
    lei.exception_occured = True

    cdef str ex_str = "Exception in pylambda function evaluation: \n"
    
    try:
        ex_str += repr(e)
    except Exception, e:
        ex_str += "Error expressing exception as string."

    ex_str += ": \n" + traceback_str
        
    lei.exception_string = ex_str

    try:
        lei.exception_pickle = py_pickle.dumps(e, protocol = -1)
    except Exception, e:
        lei.exception_pickle = py_pickle.dumps("<Error pickling exception>", protocol = -1)

    

#########################################
# Random seed

cdef void _set_random_seed(size_t seed):
    set_random_seed(seed)

eval_functions.set_random_seed = _set_random_seed


########################################
# Initialization function.

cdef size_t _init_lambda(const string& _lambda_string, lambda_exception_info* lei):

    global _lambda_counter
    global _lambda_id_to_evaluator
    global _lambda_function_string_to_id
    
    cdef str lambda_string = _lambda_string
    cdef object lmfunc_id

    try:

        lmfunc_id = _lambda_function_string_to_id.get(lambda_string, None)

        # If it's been cleared out, then erase it and restart.
        if lmfunc_id is not None and lmfunc_id not in _lambda_id_to_evaluator:
            del _lambda_function_string_to_id[lambda_string]
            lmfunc_id = None

        if lmfunc_id is None:
            _lambda_counter += 1
            lmfunc_id = _lambda_counter
            _lambda_id_to_evaluator[lmfunc_id] = lambda_evaluator(lambda_string)
            _lambda_function_string_to_id[lambda_string] = lmfunc_id

        return lmfunc_id

    except Exception, e:
        _process_exception(e, traceback.format_exc(), lei)

eval_functions.init_lambda = _init_lambda

########################################
# Release

cdef void _release_lambda(size_t lmfunc_id, lambda_exception_info* lei):

    try:
        try:
            del _lambda_function_string_to_id[lmfunc_id]
        except KeyError, ke:
            raise ValueError("Attempted to clear lambda id that does not exist (%d)" % lmfunc_id)

    except Exception, e:
        _process_exception(e, traceback.format_exc(), lei)

eval_functions.release_lambda = _release_lambda

########################################
# Single column evaluation

cdef void _eval_lambda(size_t lmfunc_id, lambda_call_data* lcd, lambda_exception_info* lei):
    try:
        _get_lambda_class(lmfunc_id).eval_simple(lcd)
    except Exception, e:
        _process_exception(e, traceback.format_exc(), lei)


eval_functions.eval_lambda = _eval_lambda

########################################
# Multiple column evaluation

cdef void _eval_lambda_by_dict(size_t lmfunc_id, lambda_call_by_dict_data* lcd, lambda_exception_info* lei):
    try:
        _get_lambda_class(lmfunc_id).eval_multiple_rows(lcd)
    except Exception, e:
        _process_exception(e, traceback.format_exc(), lei)

eval_functions.eval_lambda_by_dict = _eval_lambda_by_dict

########################################
# Multiple column evaluation

cdef void _eval_lambda_by_sframe_rows(size_t lmfunc_id, lambda_call_by_sframe_rows_data* lcd, lambda_exception_info* lei):
    try:
        _get_lambda_class(lmfunc_id).eval_sframe_rows(lcd)
    except Exception, e:
        _process_exception(e, traceback.format_exc(), lei)

eval_functions.eval_lambda_by_sframe_rows = _eval_lambda_by_sframe_rows


eval_functions_ctype_pointer = \
  ctypes.c_void_p(<long>(<void*>(&eval_functions)))
