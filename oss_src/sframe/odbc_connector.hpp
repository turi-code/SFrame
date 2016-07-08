/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_ODBC_CONNECTOR_HPP
#define GRAPHLAB_ODBC_CONNECTOR_HPP

#ifdef _WIN32
#include <cross_platform/windows_wrapper.hpp>
#endif

#include <sql.h>
#include <sqlext.h>

#include <sframe/sframe.hpp>
#include <sframe/sframe_constants.hpp>
#include <sframe/algorithm.hpp>
#include <flexible_type/flexible_type.hpp>
#include <flexible_type/flexible_type_spirit_parser.hpp>
#include <cppipc/server/cancel_ops.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/case_conv.hpp>


namespace graphlab {

// This struct is used to describe a column for reading and writing.  Not all
// of the members are used for both operations, but they all generally apply to
// a column of data. This could be extended to do some sanity checks based on
// which one is happening, but for now it serves its purpose.
typedef struct {
  std::string column_name;
  // Yes. Four types of types. 
  flex_type_enum column_type;
  SQLSMALLINT column_c_type;
  SQLSMALLINT column_sql_type;
  std::string db_specific_type;
  // I intend this to mean the number of bytes that GraphLab Create must
  // allocate to read/write one element of this column.  For string/binary
  // types, this matches ODBC's definition of "column size".  For other types,
  // it doesn't.
  size_t max_size_in_bytes;
  // For string/binary types, this will equal max_size_in_bytes.
  flex_int column_size;
  int num_decimal_digits;
  SQLSMALLINT nullable;
  SQLSMALLINT unsigned_attribute;
  SQLSMALLINT fixed_precision;
} column_desc_t;

// Used to name the columns of the output of SQLGetTypeInfo.
enum type_info_keys { TYPE_NAME = 0,
                      DATA_TYPE,
                      COLUMN_SIZE,
                      LITERAL_PREFIX,
                      LITERAL_SUFFIX,
                      CREATE_PARAMS,
                      NULLABLE,
                      CASE_SENSITIVE,
                      SEARCHABLE,
                      UNSIGNED_ATTRIBUTE,
                      FIXED_PREC_SCALE,
                      AUTO_UNIQUE_VALUE,
                      LOCAL_TYPE_NAME,
                      MINIMUM_SCALE,
                      MAXIMUM_SCALE,
                      SQL_DATA_TYPE,
                      DATETIME_SUB,
                      NUM_PREC_RADIX,
                      INTERVAL_PRECISION };

class odbc_connector {
 public:
  odbc_connector();
  ~odbc_connector();

  /**
   * Perform all intialization.
   *
   * This allocates all handles we need to live for the lifetime of this class
   * and does some global mapping for DB-specific types.
   */
  void init(const std::string &conn_str);

  void clear();

  /**
   * Inserts the data contained in an SFrame to a table in the DB.
   *
   * The default action is to check if a table with the given name exists in
   * the DB.  If it does, it will try to insert the rows to the end of the
   * table.  If not, it will create a new table for the data.  The
   * auto_create_table option will cause this method to fail if the table does
   * not exist.
   */
  void insert_data(sframe &sf, const std::string &table_name, bool append_if_exists, bool auto_create_table=true);
  
  /**
   * Sends the given query to the database and returns the result as an SFrame.
   */
  bool get_query_result_as_sframe(sframe &sf, std::string query_str=std::string(""));

  /**
   * Getters for DBMS product info (for our metrics).
   */
  std::string get_dbms_name() {
    if(SQL_SUCCEEDED(m_dbms_info_available))
      return std::string((char *)m_dbms_name);
    return std::string("");
  }

  std::string get_dbms_version() {
    if(SQL_SUCCEEDED(m_dbms_info_available))
      return std::string((char *)m_dbms_ver);
    return std::string("");
  }
 protected:
 private:
  /// Members
  SQLHENV m_env;
  SQLHDBC m_dbc;
  SQLHSTMT m_query_stmt;
  SQLRETURN m_ret;
  bool m_inited;
  bool m_query_running;
  bool m_types_mapped;
  SQLSMALLINT m_num_result_cols;
  SQLLEN m_num_rows_fetched;
  std::vector<column_desc_t> m_result_column_info;
  SQLPOINTER *m_entry_buffer;
  SQLLEN **m_entry_buffer_size;
  size_t m_num_rows_to_fetch;
  std::unordered_set<size_t> m_large_columns;
  SQLPOINTER m_large_column_buffer = NULL;
  SQLLEN m_large_column_buffer_size = 0;
  size_t m_total_allocated_for_read = 0;

  SQLCHAR *m_name_buf;
  SQLUSMALLINT m_name_buf_len;
  SQLCHAR m_identifier_quote_char[2];
  SQLCHAR m_dbms_name[64];
  SQLCHAR m_dbms_ver[64];
  int m_dbms_info_available;

  // Write-specific members
  std::vector<column_desc_t> m_column_write_info;
  SQLHSTMT m_insert_stmt;
  SQLPOINTER *m_row_bound_params;
  SQLLEN **m_value_size_indicator;
  std::vector<std::vector<flexible_type>> m_db_type_info;
  std::map<SQLSMALLINT, std::vector<size_t>> m_db_type_info_by_sql_type;
  std::map<std::string, size_t> m_db_type_info_names;
  std::map<flex_type_enum, std::vector<SQLSMALLINT>> m_flex2sql_types;
  bool m_table_created;

  /// Methods
  /**
   * Start a query that does not insert rows in the database.
   *
   * Takes the query_str and sends it to the database to execute. Then, this
   * determines whether that query will generate a result set.  If so,
   * get_query_block must be called to get the results.  In some cases, other
   * SQL commands will start a query, but this function must still be used to
   * start gathering the result set.  In this case, the already_started flag
   * should be set to true, and the query string will be ignored.
   * 
   * Returns the number of columns that will be returned in the result set (0
   * if no result set will be generated).
   */
  size_t start_query(const std::string &query_str);

  /**
   * Returns a block of results if there are some available.
   *
   * Returns an empty vector if there are no results available from the ODBC
   * driver.
   */
  std::vector<std::vector<flexible_type>> get_query_block();

  /**
   * Returns the column names of the current result set, if any.
   */
  std::vector<std::string> get_column_names();

  /**
   * Returns the column names of the current result set, if any.
   */
  std::vector<flex_type_enum> get_column_types();

  /**
   * Convert a memory region of a given SQL C type to flexible_type.
   * 
   * Used for reading. Throws if the C type or flexible_type is unsupported.
   */
  void sql_to_flexible_type(void *buf, size_t len, SQLSMALLINT sql_c_type, flexible_type &out);

  /**
   * Helper function for converting a SQL interval to a flex_dict.
   */
  void add_to_interval_dict(flex_dict &the_dict, std::string key, SQLUINTEGER value);

  /**
   * Clean up all structures associated with a read query.
   *
   * This is called from get_query_block after no more results are found.
   * Closes the cursor so the statement handle can be used again.
   */
  void finalize_query();

  /**
   * Given the row size in bytes, calculate how many rows to read/write at a time.
   *
   * Uses the user-set constant of ODBC_BUFFER_SIZE, also in bytes.
   */
  size_t calculate_batch_size(size_t max_row_size);

  /**
   * Does general mapping from ODBC SQL types to flexible_type.
   *
   * This is not the only mapping that needs to happen for writing, as some
   * column-specific stuff needs to happen too.
   */
  void map_types_for_writing();

  /**
   * Do column-specific type mapping for writes.
   *
   * This does most of the heavy lifting for picking a suitable DB-specific
   * type for the columns of the SFrame we want to add to the DB.  It also
   * finds most of the parameters we need for our call to SQLBindParameter.
   *
   * There is an "optimize_db_storage" mode which is not implemented yet.  The
   * idea is that this would go through every column and figure out how much
   * storage is actually needed for that column (for example, if an integer
   * column only stores values 0-5, it can be stored as a SQL_TINYINT, or even
   * a string, instead of the default 64-bit integer).  This would take much
   * longer to scan all the data, but would optimize for storage in your
   * database.  Will implement if there's demand.
   */
  void map_types_for_writing(sframe &sf, bool optimize_db_storage=false);

  /**
   * Stitch together a create table SQL command.
   * 
   * Must be run after types are mapped for writing.
   */
  std::string make_create_table_string(sframe &sf, const std::string &table_name);

  bool create_table(sframe &sf, const std::string &table_name);

  /**
   * Get the "limit" of each column.
   *
   * This has a different meaning based on the type.  I wish it wasn't so, but
   * it is this way because ODBC is this way.  For numeric, this returns the
   * minimum and maximum value (minimum first in the returned pair).  For
   * strings and other variable (supported) types, it returns the maximum
   * length in bytes (only maximum, as this is all that's important right now).
   *
   * This will be much more useful when DB storage optimization mode is implemented.
   */
  std::pair<flexible_type, flexible_type> get_column_limits(std::shared_ptr<sarray<flexible_type>> column, bool optimize_db_storage);
  
  /**
   * Helper functions for dealing with SQL intervals when writing.
   */
  int convert_str_to_time_code(std::string s);
  SQLSMALLINT identify_interval_type(flex_dict interval);
  void add_to_interval_struct(SQL_INTERVAL_STRUCT &s, std::pair<flexible_type, flexible_type> entry);


  /**
   * Y'know...all the actual work of inserting data.
   *
   * See insert_data.
   */
  void insert_data_impl(sframe &sf, const std::string &table_name, bool append_if_exists, bool auto_create_table);


  /**
   * A time-saving function to take a return status and do the right thing.
   *
   * If the error is fatal, this will throw an exception.  If not, it will log
   * the error to INFO_LEVEL.
   */
  void handle_return(SQLRETURN orig_ret_status,
                     const char *fn,
                     SQLHANDLE handle,
                     SQLSMALLINT type,
                     std::string user_err_msg="");

  /**
   * Free memory of dynamically allocated variables needed for insert
   */
  void finalize_insert(size_t num_columns);

  /**
   * Convert a result returned by SQLFetch to a flexible_type.
   * 
   * Works with the output of both SQLGetData and SQLFetch when the columns are bound.
   */
  void result_buffer_to_flexible_type(void *buffer_pos, SQLLEN elem_size, SQLSMALLINT c_type, flexible_type &out);
};

}

#endif // GRAPHLAB_ODBC_CONNECTOR_HPP
