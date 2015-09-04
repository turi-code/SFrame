project(table_printer_test)

make_cxxtest(table_printing.cxx REQUIRES table_printer unity_util unity_ml_data_2 unity)
