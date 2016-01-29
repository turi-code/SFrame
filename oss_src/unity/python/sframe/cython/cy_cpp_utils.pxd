from libcpp.string cimport string
from libcpp.vector cimport vector
from libcpp.map cimport map
from cpython.version cimport PY_MAJOR_VERSION

cdef inline string str_to_cpp(py_s) except *:
    """
    Use this function to convert any string-like object to the c++
    string class.
    """
    cdef type t = type(py_s)
    if PY_MAJOR_VERSION >= 3:
        if t is str:
            return (<str>py_s).encode()
        elif t is bytes:
            return (<bytes>py_s)
    else:
        if t is str:
            return (<str>py_s)
        elif t is unicode:
            return (<unicode>py_s).encode()
        else:
            raise TypeError("Cannot interpret type '%s' as native string." % str(t))

cdef inline string unsafe_str_to_cpp(py_s) except *:
    """
    Use this version if you know for sure that type(py_s) is str.
    """
    if PY_MAJOR_VERSION >= 3:
        (<str>py_s).encode()
    else:
        return (<str>py_s)

cdef inline string unsafe_unicode_to_cpp(py_s) except *:
    """
    Use this version if you know for sure that type(py_s) is unicode
    (same as str in python 3).
    """
    if PY_MAJOR_VERSION >= 3:
        (<str>py_s).encode()
    else:
        return (<unicode>py_s).encode()
            
cdef inline str cpp_to_str(const string& cpp_s):
    cdef const char* c_s = cpp_s.data()
    
    if PY_MAJOR_VERSION >= 3:
        return c_s[:cpp_s.size()].decode()
    else:
        return str(c_s[:cpp_s.size()])


cdef inline vector[string] to_vector_of_strings(object v) except *:
    cdef list fl
    cdef tuple ft
    cdef long i
    cdef vector[string] ret

    if type(v) is list:
        fl = <list>v
        ret.resize(len(fl))
        for i in range(len(fl)):
            ret[i] = str_to_cpp(fl[i])
    elif type(v) is tuple:
        ft = <tuple>v
        ret.resize(len(ft))
        for i in range(len(ft)):
            ret[i] = str_to_cpp(ft[i])
    else:
        raise TypeError("Cannot interpret type '%s' as list of strings." % str(type(v)))

cdef inline list from_vector_of_strings(const vector[string]& sv):
    cdef list ret = [None]*sv.size()
    cdef long i

    for i in range(<long>(sv.size())):
        ret[i] = cpp_to_str(sv[i])

    return ret
    
cdef inline vector[vector[string]] to_nested_vectors_of_strings(object v) except *:
    cdef list fl
    cdef tuple ft
    cdef long i
    cdef vector[vector[string]] ret

    if type(v) is list:
        fl = <list>v
        ret.resize(len(fl))
        for i in range(len(fl)):
            ret[i] = to_vector_of_strings(fl[i])
    elif type(v) is tuple:
        ft = <tuple>v
        ret.resize(len(ft))
        for i in range(len(ft)):
            ret[i] = to_vector_of_strings(ft[i])
    else:
        vs = str(v)
        if len(vs) > 50:
            vs = vs[:50] + "..."
        raise TypeError("Cannot interpret '%s' as nested lists of strings." % vs)
        
cdef inline map[string, string] dict_to_string_string_map(dict d) except *:
    cdef map[string,string] ret

    for k, v in d.iteritems():
        ret[str_to_cpp(k)] = str_to_cpp(v)

    return ret

