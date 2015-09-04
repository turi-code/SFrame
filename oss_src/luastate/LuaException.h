//
//  LuaException.h
//  LuaState
//
//  Created by Simon Mikuda on 17/03/14.
//
//  See LICENSE and README.md files

#pragma once

#include <exception>

namespace lua {
    
    namespace stack {
        
        inline void dump (lua_State *L) {
            int i;
            int top = lua_gettop(L);
            for (i = 1; i <= top; i++) {  /* repeat for each level */
                int t = lua_type(L, i);
                switch (t) {
                    case LUA_TSTRING:  /* strings */
                        printf("`%s'", lua_tostring(L, i));
                        break;
                        
                    case LUA_TBOOLEAN:  /* booleans */
                        printf(lua_toboolean(L, i) ? "true" : "false");
                        break;
                        
                    case LUA_TNUMBER:  /* numbers */
                        printf("%g", lua_tonumber(L, i));
                        break;
                        
                    default:  /* other values */
                        printf("%s", lua_typename(L, t));
                        break;
                        
                }
                printf("  ");  /* put a separator */
            }
            printf("\n");  /* end the listing */
        }
        
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////
    class LoadError: public std::exception
    {
        std::string _message;
        
    public:
        LoadError(const std::string& message) : _message(message) {}
        virtual ~LoadError() throw() {}
        
        virtual const char* what() const throw()
        {
            return _message.c_str();
        }
    };
    
    //////////////////////////////////////////////////////////////////////////////////////////////////
    /// Don't forget to call execute manualy while using protected call!
    /// Exceptions cannot be catched while throwed in destructor...
    class RuntimeError: public std::exception
    {
        std::string _message;
        
    public:
        RuntimeError(const std::string& message) : _message(message) {}
        virtual ~RuntimeError() throw() {}
        
        virtual const char* what() const throw()
        {
            return _message.c_str();
        }
    };
}
