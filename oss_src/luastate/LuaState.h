////////////////////////////////////////////////////////////////////////////////////////////////
//
//  LuaState.h
//  LuaState
//
//  Created by Simon Mikuda on 18/03/14.
//
//  See LICENSE and README.md files
//////////////////////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <cassert>
#include <string>
#include <functional>
#include <memory>
#include <tuple>
#include <cstring>
#include <cmath>
extern "C" {
#include <lua/lua.h>
#include <lua/lualib.h>
#include <lua/lauxlib.h>
}

#ifdef LUASTATE_DEBUG_MODE
#   define LUASTATE_DEBUG_LOG(format, ...) printf(format "\n", ## __VA_ARGS__)
#else
#   define LUASTATE_DEBUG_LOG(format, ...)
#endif

#include "./DeallocStackQueue.h"
#include "./Traits.h"

#include "./LuaPrimitives.h"
#include "./LuaStack.h"
#include "./LuaException.h"
#include "./LuaValue.h"
#include "./LuaReturn.h"
#include "./LuaFunctor.h"
#include "./LuaRef.h"

namespace lua {
    
    //////////////////////////////////////////////////////////////////////////////////////////////////
    /// Class that hold lua interpreter state. Lua state is managed by shared pointer which also is copied to lua::Ref values.
    class State
    {
        /// Shared pointer takes care of automaticaly closing Lua state when all instances pointing to it are gone
        std::shared_ptr<lua_State> _luaState;
        
        detail::DeallocQueue* _deallocQueue;
        
        /// Function for metatable "__call" field. It calls stored functor pushes return values to stack.
        ///
        /// @pre In Lua C API during function calls lua_State moves stack index to place, where first element is our userdata, and next elements are returned values
        static int metatableCallFunction(lua_State* luaState) {
            std::weak_ptr<lua_State> weakPtr = *(std::weak_ptr<lua_State> *)lua_topointer(luaState, lua_upvalueindex(1));
            
            std::shared_ptr<lua_State> instance = weakPtr.lock();
            if (instance != nullptr) {
                BaseFunctor* functor = *(BaseFunctor **)luaL_checkudata(luaState, 1, "luaL_Functor");;
                return functor->call(instance);
            }
            return 0;
        }
        
        /// Function for metatable "__gc" field. It deletes captured variables from stored functors.
        static int metatableDeleteFunction(lua_State* luaState) {
            BaseFunctor* functor = *(BaseFunctor **)luaL_checkudata(luaState, 1, "luaL_Functor");;
            delete functor;
            return 0;
        }
        
    public:
        
        /// Constructor creates new state and stores it to shared pointer.
        ///
        /// @param loadLibs     If we want to open standard libraries - function luaL_openlibs
        State(bool loadLibs) {
            
            _deallocQueue = new detail::DeallocQueue();
            detail::DEALLOC_QUEUE = _deallocQueue;
            lua_State* luaState = luaL_newstate();
            assert(luaState != NULL);
            
            if (loadLibs)
                luaL_openlibs(luaState);
            
            // Set up destructor for lua state, function lua_close and deallocQueue
            std::weak_ptr<lua_State>** weakPtr = new std::weak_ptr<lua_State>*();
            _luaState = std::shared_ptr<lua_State>(luaState, [this, weakPtr] (lua_State* L) {
                lua_close(L);
                delete _deallocQueue;
                delete *weakPtr;
                delete weakPtr;
            });
            *weakPtr = new std::weak_ptr<lua_State>(_luaState);
            
            // We will create metatable for Lua functors for memory management and actual function call
            luaL_newmetatable(luaState, "luaL_Functor");
            
            // Set up metatable call operator for functors
            lua_pushlightuserdata(luaState, *weakPtr);
            lua_pushcclosure(luaState, &State::metatableCallFunction, 1);
            lua_setfield(luaState, -2, "__call");
            
            // Set up metatable garbage collection for functors
            lua_pushcfunction(luaState, &State::metatableDeleteFunction);
            lua_setfield(luaState, -2, "__gc");
            
            // Pop metatable
            lua_pop(luaState, 1);
        }
        
        /// Constructor creates new state stores it to shared pointer and loads standard libraries
        State() : State(true) {}
        
        /// Query global values from Lua state
        ///
        /// @return Some value with type lua::Type
        Value operator[](lua::String name) {
            return Value(_luaState, _deallocQueue, name);
        }
        
        /// Deleted compare operator
        bool operator==(Value &other) = delete;
        
        /// Sets global value to Lua state
        ///
        /// @param key      Stores value to _G[key]
        /// @param value    Value witch will be stored to _G[key]
        template<typename T>
        void set(lua::String key, const T& value) const {
            stack::push(_luaState, value);
            lua_setglobal(_luaState.get(), key);
        }
        
        /// Executes file text on Lua state
        ///
        /// @throws lua::LoadError      When file cannot be found or loaded
        /// @throws lua::RuntimeError   When there is runtime error
        ///
        /// @param filePath File path indicating which file will be executed
        void doFile(const std::string& filePath) {
            if (luaL_loadfile(_luaState.get(), filePath.c_str())) {
                std::string message = stack::read<std::string>(_luaState, -1);
                stack::pop(_luaState, 1);
                throw LoadError(message);
            }
            if (lua_pcall(_luaState.get(), 0, LUA_MULTRET, 0)) {
                std::string message = stack::read<std::string>(_luaState, -1);
                stack::pop(_luaState, 1);
                throw RuntimeError(message);
            }
        }
        
        /// Execute string on Lua state
        ///
        /// @throws lua::LoadError      When string cannot be loaded
        /// @throws lua::RuntimeError   When there is runtime error
        ///
        /// @param string   Command which will be executed
        void doString(const std::string& string) {
            if (luaL_loadstring(_luaState.get(), string.c_str())) {
                std::string message = stack::read<std::string>(_luaState, -1);
                stack::pop(_luaState, 1);
                throw LoadError(message);
            }
            if (lua_pcall(_luaState.get(), 0, LUA_MULTRET, 0)) {
                std::string message = stack::read<std::string>(_luaState, -1);
                stack::pop(_luaState, 1);
                throw RuntimeError(message);
            }
        }

#ifdef LUASTATE_DEBUG_MODE
        
        /// Flush all elements from stack and check ref counting
        void checkMemLeaks() {
            
            LUASTATE_DEBUG_LOG("Reference counter is %d", REF_COUNTER);
            
            int count = stack::top(_luaState);
            LUASTATE_DEBUG_LOG("Flushed %d elements from stack:", count);
            stack::dump(_luaState.get());
            lua_settop(_luaState.get(), 0);

            LUASTATE_DEBUG_LOG("Deallocation queue has %lu elements", _deallocQueue->size());
            assert(_deallocQueue->empty());
            
            // Check for memory leaks during ref counting, should be zero
            assert(REF_COUNTER == 0);
            
            // Check if there are any values from stack, should be zero
            assert(count == 0);
        }
        
        void stackDump() {
            lua::stack::dump(_luaState.get());
        }
#endif
        
        /// Get shared pointer of Lua state
        ///
        /// @return Shared pointer of Lua state
        std::shared_ptr<lua_State> getState() { return _luaState; }
    };
}
