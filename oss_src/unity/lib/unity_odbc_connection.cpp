/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <unity/lib/unity_odbc_connection.hpp>
#include <sframe/sframe.hpp>

namespace graphlab {
namespace odbc_connection {

class unity_odbc_connection : public graphlab::toolkit_class_base {
 public:

  std::shared_ptr<unity_sframe_base> execute_query(const std::string &query) {
    log_func_entry();
    std::shared_ptr<unity_sframe> ret(new unity_sframe());

    sframe sf;

    bool result = m_db_connector.get_query_result_as_sframe(sf, query);

    if(result)
      ret->construct_from_sframe(sf);

    return ret;
  }

  void _construct_from_odbc_conn_str(const std::string &conn_str) {
    log_func_entry();

    m_db_connector.init(conn_str);
    dbms_name = m_db_connector.get_dbms_name();
    dbms_version = m_db_connector.get_dbms_version();
  }

  void _insert_sframe(std::shared_ptr<unity_sframe_base> sf, const std::string &table_name, bool append_if_exists) {
    log_func_entry();
    logstream(LOG_INFO) << "append: " << append_if_exists << std::endl;
    auto sf_derived = std::dynamic_pointer_cast<unity_sframe>(sf);

    auto real_sf = sf_derived->get_underlying_sframe();
    m_db_connector.insert_data(*real_sf, table_name, append_if_exists);
  }

 protected:
 private:
  odbc_connector m_db_connector;
  void save_impl(oarchive& oarc) const {}
  void load_version(iarchive& iarc, size_t version) {}
  std::string dbms_name;
  std::string dbms_version;

 public:
  BEGIN_CLASS_MEMBER_REGISTRATION("unity_odbc_connection")
  REGISTER_CLASS_MEMBER_FUNCTION(unity_odbc_connection::_construct_from_odbc_conn_str, "conn_str")
  REGISTER_CLASS_MEMBER_FUNCTION(unity_odbc_connection::execute_query, "query_str")
  REGISTER_CLASS_MEMBER_DOCSTRING(unity_odbc_connection::execute_query,
      "Execute any query against the database connection.\n"
      "\n"
      "This function will always return an SFrame, even if the query does not\n"
      "return any rows.\n"
      "\n"
      "Parameters\n"
      "----------\n"
      "query_str : str\n"
      "  The query string to be accepted by the database.  Usually it is in\n"
      "  SQL, but it can be whatever your ODBC driver will accept.\n"
      "\n"
      "Returns\n"
      "-------\n"
      "out : graphlab.SFrame");
  REGISTER_CLASS_MEMBER_FUNCTION(unity_odbc_connection::_insert_sframe, "sf", "table_name", "append_if_exists")
  REGISTER_PROPERTY(dbms_name)
  REGISTER_PROPERTY(dbms_version)
  END_CLASS_MEMBER_REGISTRATION
};

BEGIN_CLASS_REGISTRATION
REGISTER_CLASS(unity_odbc_connection)
END_CLASS_REGISTRATION
}
}
