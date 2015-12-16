/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <sframe/sframe_constants.hpp>
#include <logger/logger.hpp>
#ifndef _WIN32
#include <dlfcn.h>
#else
#include <process/gl_windows.hpp>
#include <util/syserr_reporting.hpp>
#endif
#include <cstring>
#include <vector>
#include <string>
#include <boost/filesystem.hpp>

#include <sql.h>
#include <sqlext.h>

namespace fs = boost::filesystem;

extern "C" {
#ifndef _WIN32
  static void* libodbc_handle = NULL;
#else
  static HINSTANCE libodbc_handle = NULL;
#endif
  bool odbc_dlopen_fail = false;

#if (ODBCVER >= 0x0300)
  static SQLRETURN  SQL_API (*ptr_SQLGetDiagRec)(SQLSMALLINT HandleType, SQLHANDLE Handle,
                                   SQLSMALLINT RecNumber, SQLCHAR *Sqlstate,
                                   SQLINTEGER *NativeError, SQLCHAR *MessageText,
                                   SQLSMALLINT BufferLength, SQLSMALLINT *TextLength) = NULL;
  static SQLRETURN  SQL_API (*ptr_SQLFreeHandle)(SQLSMALLINT HandleType, SQLHANDLE Handle) = NULL;
  static SQLRETURN  SQL_API (*ptr_SQLAllocHandle)(SQLSMALLINT HandleType,
                                    SQLHANDLE InputHandle, SQLHANDLE *OutputHandle) = NULL;
  static SQLRETURN  SQL_API (*ptr_SQLSetEnvAttr)(SQLHENV EnvironmentHandle,
                                   SQLINTEGER Attribute, SQLPOINTER Value,
                                   SQLINTEGER StringLength) = NULL;
  static SQLRETURN  SQL_API (*ptr_SQLGetTypeInfo)(SQLHSTMT StatementHandle,
                                    SQLSMALLINT DataType) = NULL;
  static SQLRETURN  SQL_API (*ptr_SQLCloseCursor)(SQLHSTMT StatementHandle) = NULL;
  static SQLRETURN  SQL_API (*ptr_SQLSetConnectAttr)(SQLHDBC ConnectionHandle,
                                       SQLINTEGER Attribute, SQLPOINTER Value,
                                       SQLINTEGER StringLength) = NULL;
  static SQLRETURN  SQL_API (*ptr_SQLGetStmtAttr)(SQLHSTMT StatementHandle,
                                    SQLINTEGER Attribute, SQLPOINTER Value,
                                    SQLINTEGER BufferLength, SQLINTEGER *StringLength) = NULL;
  static SQLRETURN  SQL_API (*ptr_SQLSetStmtAttr)(SQLHSTMT StatementHandle,
                                    SQLINTEGER Attribute, SQLPOINTER Value,
                                    SQLINTEGER StringLength) = NULL;
  static SQLRETURN  SQL_API (*ptr_SQLEndTran)(SQLSMALLINT HandleType, SQLHANDLE Handle,
                                SQLSMALLINT CompletionType) = NULL;
#endif
  static SQLRETURN  SQL_API (*ptr_SQLDisconnect)(SQLHDBC ConnectionHandle) = NULL;
  static SQLRETURN SQL_API (*ptr_SQLDriverConnect)(
      SQLHDBC            hdbc,
      SQLHWND            hwnd,
      SQLCHAR 		  *szConnStrIn,
      SQLSMALLINT        cbConnStrIn,
      SQLCHAR           *szConnStrOut,
      SQLSMALLINT        cbConnStrOutMax,
      SQLSMALLINT 	  *pcbConnStrOut,
      SQLUSMALLINT       fDriverCompletion) = NULL;
  static SQLRETURN  SQL_API (*ptr_SQLExecDirect)(SQLHSTMT StatementHandle,
                                   SQLCHAR *StatementText, SQLINTEGER TextLength) = NULL;
  static SQLRETURN  SQL_API (*ptr_SQLNumResultCols)(SQLHSTMT StatementHandle,
                                      SQLSMALLINT *ColumnCount) = NULL;
  static SQLRETURN  SQL_API (*ptr_SQLDescribeCol)(SQLHSTMT StatementHandle,
                                    SQLUSMALLINT ColumnNumber, SQLCHAR *ColumnName,
                                    SQLSMALLINT BufferLength, SQLSMALLINT *NameLength,
                                    SQLSMALLINT *DataType, SQLULEN *ColumnSize,
                                    SQLSMALLINT *DecimalDigits, SQLSMALLINT *Nullable) = NULL;
  static SQLRETURN  SQL_API (*ptr_SQLFetch)(SQLHSTMT StatementHandle) = NULL;
  static SQLRETURN  SQL_API (*ptr_SQLGetData)(SQLHSTMT StatementHandle,
                                SQLUSMALLINT ColumnNumber, SQLSMALLINT TargetType,
                                SQLPOINTER TargetValue, SQLLEN BufferLength,
                                SQLLEN *StrLen_or_Ind) = NULL;
  static SQLRETURN   SQL_API (*ptr_SQLTables)(SQLHSTMT StatementHandle,
                                SQLCHAR *CatalogName, SQLSMALLINT NameLength1,
                                SQLCHAR *SchemaName, SQLSMALLINT NameLength2,
                                SQLCHAR *TableName, SQLSMALLINT NameLength3,
                                SQLCHAR *TableType, SQLSMALLINT NameLength4) = NULL;
  static SQLRETURN SQL_API (*ptr_SQLBindParameter)(
      SQLHSTMT           hstmt,
      SQLUSMALLINT       ipar,
      SQLSMALLINT        fParamType,
      SQLSMALLINT        fCType,
      SQLSMALLINT        fSqlType,
      SQLULEN            cbColDef,
      SQLSMALLINT        ibScale,
      SQLPOINTER         rgbValue,
      SQLLEN             cbValueMax,
      SQLLEN 		      *pcbValue) = NULL;
  static SQLRETURN  SQL_API (*ptr_SQLPrepare)(SQLHSTMT StatementHandle,
      SQLCHAR *StatementText, SQLINTEGER TextLength) = NULL;
  static SQLRETURN  SQL_API (*ptr_SQLExecute)(SQLHSTMT StatementHandle) = NULL;
  static SQLRETURN  SQL_API (*ptr_SQLBindCol)(SQLHSTMT StatementHandle,
                                SQLUSMALLINT ColumnNumber, SQLSMALLINT TargetType,
                                SQLPOINTER TargetValue, SQLLEN BufferLength,
                                SQLLEN *StrLen_or_Ind) = NULL;
  static SQLRETURN  SQL_API (*ptr_SQLGetInfo)(SQLHDBC ConnectionHandle,
                                SQLUSMALLINT InfoType, SQLPOINTER InfoValue,
                                SQLSMALLINT BufferLength, SQLSMALLINT *StringLength) = NULL;
  static SQLRETURN  SQL_API (*ptr_SQLFreeStmt)(SQLHSTMT StatementHandle,
                                   SQLUSMALLINT Option) = NULL;

  static void connect_odbc_shim() {
    static bool shim_attempted = false;
    if (shim_attempted == false) {
      shim_attempted = true;
      const char *err_msg;

      std::vector<fs::path> potential_paths = {
        // Look in the user-specified location
        fs::path(graphlab::LIBODBC_PREFIX + "/libodbc.so"),
        fs::path(graphlab::LIBODBC_PREFIX + "/libodbc.dylib"),
        // find the system-installed so
        fs::path("libodbc.so"),
        fs::path("libodbc.dylib"),
      };

#ifdef _WIN32
      for(auto &i : potential_paths) {
        i.replace_extension(".dll");
      }
#endif

      // We don't want to freak customers out with failure messages as we try
      // to load these libraries.  There's going to be a few in the normal
      // case. Print them if we really didn't find libodbc.so.
      std::vector<std::string> error_messages;

      for(auto &i : potential_paths) {
        logstream(LOG_INFO) << "Trying " << i << std::endl;
#ifndef _WIN32
        libodbc_handle = dlopen(i.native().c_str(), RTLD_NOW | RTLD_LOCAL);
#else
        libodbc_handle = LoadLibrary(i.string().c_str());
#endif

        if(libodbc_handle != NULL) {
          logstream(LOG_INFO) << "Success!" << std::endl;
          break;
        } else {
#ifndef _WIN32
          err_msg = dlerror();
          if(err_msg != NULL) {
            error_messages.push_back(std::string(err_msg));
          } else {
            error_messages.push_back(std::string("dlerror returned NULL"));
          }
#else
          error_messages.push_back(get_last_err_str(GetLastError()));
#endif
        }
      }

      if (libodbc_handle == NULL) {
        logstream(LOG_INFO) << "Unable to load libodbc.{so,dylib}" << std::endl;
        odbc_dlopen_fail = true;
        for(size_t i = 0; i < potential_paths.size(); ++i) {
          logstream(LOG_INFO) << error_messages[i] << std::endl;
        }
      }
    }
  }

  static void* get_symbol(const char* symbol) {
    connect_odbc_shim();
    if (odbc_dlopen_fail || libodbc_handle == NULL) return NULL;
#ifndef _WIN32
    return dlsym(libodbc_handle, symbol);
#else
    return (void *)GetProcAddress(libodbc_handle, symbol);
#endif
  }

  SQLRETURN  SQL_API SQLGetDiagRec(SQLSMALLINT HandleType, SQLHANDLE Handle,
                                   SQLSMALLINT RecNumber, SQLCHAR *Sqlstate,
                                   SQLINTEGER *NativeError, SQLCHAR *MessageText,
                                   SQLSMALLINT BufferLength, SQLSMALLINT *TextLength) {
    if (!ptr_SQLGetDiagRec) *(void**)(&ptr_SQLGetDiagRec) = get_symbol("SQLGetDiagRec");
    if (ptr_SQLGetDiagRec)
      return ptr_SQLGetDiagRec(HandleType,Handle,RecNumber,Sqlstate,NativeError,MessageText,BufferLength,TextLength);
    else {
      return SQL_ERROR;
    }
  }
  SQLRETURN  SQL_API SQLFreeHandle(SQLSMALLINT HandleType, SQLHANDLE Handle) {
    if (!ptr_SQLFreeHandle) *(void**)(&ptr_SQLFreeHandle) = get_symbol("SQLFreeHandle");
    if (ptr_SQLFreeHandle) return ptr_SQLFreeHandle(HandleType, Handle);
    else return SQL_ERROR;
  }
  SQLRETURN  SQL_API SQLAllocHandle(SQLSMALLINT HandleType,
                                    SQLHANDLE InputHandle, SQLHANDLE *OutputHandle) {
    if (!ptr_SQLAllocHandle) *(void**)(&ptr_SQLAllocHandle) = get_symbol("SQLAllocHandle");
    if (ptr_SQLAllocHandle) return ptr_SQLAllocHandle(HandleType,
                                    InputHandle, OutputHandle);
    else return SQL_ERROR;
  }
  SQLRETURN  SQL_API SQLSetEnvAttr(SQLHENV EnvironmentHandle,
                                   SQLINTEGER Attribute, SQLPOINTER Value,
                                   SQLINTEGER StringLength) {
    if (!ptr_SQLSetEnvAttr) *(void**)(&ptr_SQLSetEnvAttr) = get_symbol("SQLSetEnvAttr");
    if (ptr_SQLSetEnvAttr) return ptr_SQLSetEnvAttr(EnvironmentHandle,
                                   Attribute, Value,
                                   StringLength);
    else return SQL_ERROR;
  }
  SQLRETURN  SQL_API SQLGetTypeInfo(SQLHSTMT StatementHandle,
                                    SQLSMALLINT DataType) {
    if (!ptr_SQLGetTypeInfo) *(void**)(&ptr_SQLGetTypeInfo) = get_symbol("SQLGetTypeInfo");
    if (ptr_SQLGetTypeInfo) return ptr_SQLGetTypeInfo(StatementHandle,DataType);
    else return SQL_ERROR;
  }
  SQLRETURN  SQL_API SQLCloseCursor(SQLHSTMT StatementHandle) {
    if (!ptr_SQLCloseCursor) *(void**)(&ptr_SQLCloseCursor) = get_symbol("SQLCloseCursor");
    if (ptr_SQLCloseCursor) return ptr_SQLCloseCursor(StatementHandle);
    else return SQL_ERROR;
  }
  SQLRETURN  SQL_API SQLSetConnectAttr(SQLHDBC ConnectionHandle,
                                       SQLINTEGER Attribute, SQLPOINTER Value,
                                       SQLINTEGER StringLength) {
    if (!ptr_SQLSetConnectAttr) *(void**)(&ptr_SQLSetConnectAttr) = get_symbol("SQLSetConnectAttr");
    if (ptr_SQLSetConnectAttr) return ptr_SQLSetConnectAttr(ConnectionHandle, Attribute,Value, StringLength);
    else return SQL_ERROR;
  }
  SQLRETURN  SQL_API SQLGetStmtAttr(SQLHSTMT StatementHandle,
                                    SQLINTEGER Attribute, SQLPOINTER Value,
                                    SQLINTEGER BufferLength, SQLINTEGER *StringLength) {
    if (!ptr_SQLGetStmtAttr) *(void**)(&ptr_SQLGetStmtAttr) = get_symbol("SQLGetStmtAttr");
    if (ptr_SQLGetStmtAttr) return ptr_SQLGetStmtAttr(StatementHandle, Attribute, Value, BufferLength, StringLength);
    else return SQL_ERROR;
  }
  SQLRETURN  SQL_API SQLSetStmtAttr(SQLHSTMT StatementHandle,
                                    SQLINTEGER Attribute, SQLPOINTER Value,
                                    SQLINTEGER StringLength) {
    if (!ptr_SQLSetStmtAttr) *(void**)(&ptr_SQLSetStmtAttr) = get_symbol("SQLSetStmtAttr");
    if (ptr_SQLSetStmtAttr) return ptr_SQLSetStmtAttr(StatementHandle, Attribute, Value, StringLength);
    else return SQL_ERROR;
  }
  SQLRETURN  SQL_API SQLEndTran(SQLSMALLINT HandleType, SQLHANDLE Handle,
                                SQLSMALLINT CompletionType) {
    if (!ptr_SQLEndTran) *(void**)(&ptr_SQLEndTran) = get_symbol("SQLEndTran");
    if (ptr_SQLEndTran) return ptr_SQLEndTran(HandleType, Handle, CompletionType);
    else return SQL_ERROR;
  }
  SQLRETURN  SQL_API SQLDisconnect(SQLHDBC ConnectionHandle) {
    if (!ptr_SQLDisconnect) *(void**)(&ptr_SQLDisconnect) = get_symbol("SQLDisconnect");
    if (ptr_SQLDisconnect) return ptr_SQLDisconnect(ConnectionHandle);
    else return SQL_ERROR;
  }
  SQLRETURN SQL_API SQLDriverConnect(
      SQLHDBC            hdbc,
      SQLHWND            hwnd,
      SQLCHAR 		  *szConnStrIn,
      SQLSMALLINT        cbConnStrIn,
      SQLCHAR           *szConnStrOut,
      SQLSMALLINT        cbConnStrOutMax,
      SQLSMALLINT 	  *pcbConnStrOut,
      SQLUSMALLINT       fDriverCompletion) {
    if (!ptr_SQLDriverConnect) *(void**)(&ptr_SQLDriverConnect) = get_symbol("SQLDriverConnect");
    if (ptr_SQLDriverConnect) return ptr_SQLDriverConnect(hdbc,hwnd,szConnStrIn,cbConnStrIn,szConnStrOut,cbConnStrOutMax,pcbConnStrOut,fDriverCompletion);
    else return SQL_ERROR;
  }
  SQLRETURN  SQL_API SQLExecDirect(SQLHSTMT StatementHandle,
                                   SQLCHAR *StatementText, SQLINTEGER TextLength) {
    if (!ptr_SQLExecDirect) *(void**)(&ptr_SQLExecDirect) = get_symbol("SQLExecDirect");
    if (ptr_SQLExecDirect) return ptr_SQLExecDirect(StatementHandle, StatementText, TextLength);
    else return SQL_ERROR;
  }
  SQLRETURN  SQL_API SQLNumResultCols(SQLHSTMT StatementHandle,
                                      SQLSMALLINT *ColumnCount) {
    if (!ptr_SQLNumResultCols) *(void**)(&ptr_SQLNumResultCols) = get_symbol("SQLNumResultCols");
    if (ptr_SQLNumResultCols) return ptr_SQLNumResultCols(StatementHandle,ColumnCount);
    else return SQL_ERROR;
  }
  SQLRETURN  SQL_API SQLDescribeCol(SQLHSTMT StatementHandle,
                                    SQLUSMALLINT ColumnNumber, SQLCHAR *ColumnName,
                                    SQLSMALLINT BufferLength, SQLSMALLINT *NameLength,
                                    SQLSMALLINT *DataType, SQLULEN *ColumnSize,
                                    SQLSMALLINT *DecimalDigits, SQLSMALLINT *Nullable) {
    if (!ptr_SQLDescribeCol) *(void**)(&ptr_SQLDescribeCol) = get_symbol("SQLDescribeCol");
    if (ptr_SQLDescribeCol) return ptr_SQLDescribeCol(StatementHandle,ColumnNumber,ColumnName,BufferLength,NameLength,DataType,ColumnSize,DecimalDigits,Nullable);
    else return SQL_ERROR;
  }
  SQLRETURN  SQL_API SQLFetch(SQLHSTMT StatementHandle) {
    if (!ptr_SQLFetch) *(void**)(&ptr_SQLFetch) = get_symbol("SQLFetch");
    if (ptr_SQLFetch) return ptr_SQLFetch(StatementHandle);
    else return SQL_ERROR;
  }
  SQLRETURN  SQL_API SQLGetData(SQLHSTMT StatementHandle,
                                SQLUSMALLINT ColumnNumber, SQLSMALLINT TargetType,
                                SQLPOINTER TargetValue, SQLLEN BufferLength,
                                SQLLEN *StrLen_or_Ind) {
    if (!ptr_SQLGetData) *(void**)(&ptr_SQLGetData) = get_symbol("SQLGetData");
    if (ptr_SQLGetData) return ptr_SQLGetData(StatementHandle,ColumnNumber,TargetType,TargetValue,BufferLength,StrLen_or_Ind);
    else return SQL_ERROR;
  }
  SQLRETURN   SQL_API SQLTables(SQLHSTMT StatementHandle,
                                SQLCHAR *CatalogName, SQLSMALLINT NameLength1,
                                SQLCHAR *SchemaName, SQLSMALLINT NameLength2,
                                SQLCHAR *TableName, SQLSMALLINT NameLength3,
                                SQLCHAR *TableType, SQLSMALLINT NameLength4) {
    if (!ptr_SQLTables) *(void**)(&ptr_SQLTables) = get_symbol("SQLTables");
    if (ptr_SQLTables) return ptr_SQLTables(StatementHandle,CatalogName,NameLength1,SchemaName,NameLength2,TableName,NameLength3,TableType,NameLength4);
    else return SQL_ERROR;
  }
  SQLRETURN SQL_API SQLBindParameter(
      SQLHSTMT           hstmt,
      SQLUSMALLINT       ipar,
      SQLSMALLINT        fParamType,
      SQLSMALLINT        fCType,
      SQLSMALLINT        fSqlType,
      SQLULEN            cbColDef,
      SQLSMALLINT        ibScale,
      SQLPOINTER         rgbValue,
      SQLLEN             cbValueMax,
      SQLLEN 		         *pcbValue) {
    if (!ptr_SQLBindParameter) *(void**)(&ptr_SQLBindParameter) = get_symbol("SQLBindParameter");
    if (ptr_SQLBindParameter) return ptr_SQLBindParameter(hstmt,ipar,fParamType,fCType,fSqlType,cbColDef,ibScale,rgbValue,cbValueMax,pcbValue);
    else return SQL_ERROR;
  }
  SQLRETURN  SQL_API SQLPrepare(SQLHSTMT StatementHandle,
      SQLCHAR *StatementText, SQLINTEGER TextLength) {
    if (!ptr_SQLPrepare) *(void**)(&ptr_SQLPrepare) = get_symbol("SQLPrepare");
    if (ptr_SQLPrepare) return ptr_SQLPrepare(StatementHandle,StatementText,TextLength);
    else return SQL_ERROR;
  }
  SQLRETURN  SQL_API SQLExecute(SQLHSTMT StatementHandle) {
    if (!ptr_SQLExecute) *(void**)(&ptr_SQLExecute) = get_symbol("SQLExecute");
    if (ptr_SQLExecute) return ptr_SQLExecute(StatementHandle);
    else return SQL_ERROR;
  }


  SQLRETURN  SQL_API SQLBindCol(SQLHSTMT StatementHandle,
                                SQLUSMALLINT ColumnNumber, SQLSMALLINT TargetType,
                                SQLPOINTER TargetValue, SQLLEN BufferLength,
                                SQLLEN *StrLen_or_Ind) {
    if (!ptr_SQLBindCol) *(void**)(&ptr_SQLBindCol) = get_symbol("SQLBindCol");
    if (ptr_SQLBindCol) return ptr_SQLBindCol(StatementHandle, ColumnNumber, TargetType, TargetValue, BufferLength, StrLen_or_Ind);
    else return SQL_ERROR;
  }


  SQLRETURN  SQL_API SQLGetInfo(SQLHDBC ConnectionHandle,
                                SQLUSMALLINT InfoType, SQLPOINTER InfoValue,
                                SQLSMALLINT BufferLength, SQLSMALLINT *StringLength) {

    if (!ptr_SQLGetInfo) *(void**)(&ptr_SQLGetInfo) = get_symbol("SQLGetInfo");
    if (ptr_SQLGetInfo) return ptr_SQLGetInfo(ConnectionHandle, InfoType, InfoValue, BufferLength, StringLength);
    else return SQL_ERROR;
  }

  SQLRETURN SQL_API SQLFreeStmt(SQLHSTMT StatementHandle, SQLUSMALLINT Option) {
    if(!ptr_SQLFreeStmt) *(void **)(&ptr_SQLFreeStmt) = get_symbol("SQLFreeStmt");
    if(ptr_SQLFreeStmt) return ptr_SQLFreeStmt(StatementHandle, Option);
    else return SQL_ERROR;
  }
}
