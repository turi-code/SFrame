//
//  LuaPrimitives.h
//  LuaState
//
//  Created by Simon Mikuda on 18/03/14.
//
//  See LICENSE and README.md files

#pragma once

namespace lua
{
    /// Lua table type
    struct Table {};
    
    /// Any Lua function, C function, or table/userdata with __call methametod
    struct Callable {};
    
    typedef lua_Number Number;
    
    typedef int Integer;
    
    typedef bool Boolean;
    
    typedef const char* String;
    
    typedef std::nullptr_t Nil;
    
    typedef void* Pointer;
}
