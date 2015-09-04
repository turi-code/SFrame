//
//  state_tests.h
//  LuaState
//
//  Created by Simon Mikuda on 16/04/14.
//
//  See LICENSE and README.md files

#include "test.h"

#include <fstream>

//////////////////////////////////////////////////////////////////////////////////////////////////
int main(int argc, char** argv)
{
    lua::State state;

    state.doString("number = 10");
    state.doString("assert(number == 10)");
    
    try {
        state.doString("we will invoke syntax error");
        assert(false);
    } catch (lua::LoadError ex) {
        printf("%s\n", ex.what());
    }
    
    try {
        state.doString("nofunction()");
        assert(false);
    } catch (lua::RuntimeError ex) {
        printf("%s\n", ex.what());
    }
    
    std::ofstream luaFile;

    try {
        state.doFile("no_file_here");
        assert(false);
    } catch (lua::LoadError ex) {
        printf("%s ", ex.what());
    }

    luaFile.open("test.lua");
    luaFile << "local number = 100; assert(number == 100)" << std::endl;
    luaFile.close();
    state.doFile("test.lua");
    
    luaFile.open("test.lua");
    luaFile << "nofunction()" << std::endl;
    luaFile.close();
    try {
        state.doFile("test.lua");
        assert(false);
    } catch (lua::RuntimeError ex) {
        printf("%s\n", ex.what());
    }
    
    state.checkMemLeaks();
    return 0;
}
