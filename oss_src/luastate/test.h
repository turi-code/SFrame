//
//  test.h
//  LuaState
//
//  Created by Simon Mikuda on 16/04/14.
//
//  See LICENSE and README.md files

#define LUASTATE_DEBUG_MODE
#include "luastate/LuaState.h"

#include <iostream>

//////////////////////////////////////////////////////////////////////////////////////////////////
static const char* createVariables = R"(

integer = 10
number = 2.5
char = 'a'
text = "hello"
boolean = true

table = {
    100, 'hello', true,
    a = 'a', b = 'b', c = 'c',
    one = 1, two = 2, three = 3,
}

nested = { ["table"] = table }
nested.nested = nested

)";

//////////////////////////////////////////////////////////////////////////////////////////////////
static const char* createFunctions = R"(

function getInteger()
    return 10
end

function getTable()
    return table
end

function getNested()
    nested.func = getNested
    return nested
end

function getValues()
    return 1, 2, 3
end

function getNestedValues()
    return 1, {1, 2, 3}, 3
end

function pack(...)
    return {...}
end

)";

