//
//  LuaFunctor.h
//  LuaState
//
//  Created by Simon Mikuda on 22/03/14.
//
//  See LICENSE and README.md files
//

#pragma once

namespace lua {
    
    //////////////////////////////////////////////////////////////////////////////////////////////////
    /// Base functor class with call function. It is used for registering lamdas, or regular functions
    struct BaseFunctor
    {
        BaseFunctor() {
            LUASTATE_DEBUG_LOG("Functor %p created!", this);
        }
        
        virtual ~BaseFunctor() {
            LUASTATE_DEBUG_LOG("Functor %p destructed!", this);
        }
        
        /// In Lua numbers of argumens can be different so here we will handle these situations.
        ///
        /// @param luaState     Shared pointer of Lua state
        inline void prepareFunctionCall(const std::shared_ptr<lua_State>& luaState, int requiredValues) {
            
            // First item is our pushed userdata
            if (stack::top(luaState) > requiredValues + 1) {
                stack::settop(luaState, requiredValues + 1);
            }
        }
        
        /// Virtual function that will make Lua call to our functor.
        ///
        /// @param luaState     Shared pointer of Lua state
        /// @param deallocQueue Queue for deletion values initialized from given luaState
        virtual int call(const std::shared_ptr<lua_State>& luaState) = 0;
    };
    
    //////////////////////////////////////////////////////////////////////////////////////////////////
    /// Functor with return values
    template <typename Ret, typename ... Args>
    struct Functor : public BaseFunctor {
        std::function<Ret(Args...)> function;
        
        /// Constructor creates functor to be pushed to Lua interpret
        Functor(std::function<Ret(Args...)> function) : BaseFunctor(), function(function) {}
        
        /// We will make Lua call to our functor.
        ///
        /// @note When we call function from Lua to C, they have their own stack, where in the first position is our binded userdata and next position are pushed arguments
        ///
        /// @param luaState     Shared pointer of Lua state
        /// @param deallocQueue Queue for deletion values initialized from given luaState
        int call(const std::shared_ptr<lua_State>& luaState) {
            Ret value = traits::apply(function, stack::get_and_pop<Args...>(luaState, nullptr, 2));
            return stack::push(luaState, value);
        }
    };
    
    //////////////////////////////////////////////////////////////////////////////////////////////////
    /// Functor with no return values
    template <typename ... Args>
    struct Functor<void, Args...> : public BaseFunctor {
        std::function<void(Args...)> function;
        
        /// Constructor creates functor to be pushed to Lua interpret
        Functor(std::function<void(Args...)> function) : BaseFunctor(), function(function) {}
        
        /// We will make Lua call to our functor.
        ///
        /// @note When we call function from Lua to C, they have their own stack, where in the first position is our binded userdata and next position are pushed arguments
        ///
        /// @param luaState     Shared pointer of Lua state
        /// @param deallocQueue Queue for deletion values initialized from given luaState
        int call(const std::shared_ptr<lua_State>& luaState) {
            traits::apply(function, stack::get_and_pop<Args...>(luaState, nullptr, 2));
            return 0;
        }
    };
    
    namespace stack {
        
        template <typename Ret, typename ... Args>
        inline int push(const std::shared_ptr<lua_State>& luaState, Ret(*function)(Args...)) {
            BaseFunctor** udata = (BaseFunctor **)lua_newuserdata(luaState.get(), sizeof(BaseFunctor *));
            *udata = new Functor<Ret, Args...>(function);
            
            luaL_getmetatable(luaState.get(), "luaL_Functor");
            lua_setmetatable(luaState.get(), -2);
            return 1;
        }
        
        template <typename Ret, typename ... Args>
        inline int push(const std::shared_ptr<lua_State>& luaState, std::function<Ret(Args...)> function) {
            BaseFunctor** udata = (BaseFunctor **)lua_newuserdata(luaState.get(), sizeof(BaseFunctor *));
            *udata = new Functor<Ret, Args...>(function);
            
            luaL_getmetatable(luaState.get(), "luaL_Functor");
            lua_setmetatable(luaState.get(), -2);
            return 1;
        }
        
        template<typename T>
        inline int push(const std::shared_ptr<lua_State>& luaState, T function)
        {
            push(luaState, (typename traits::function_traits<T>::Function)(function));
            return 1;
        }
        
    }
}
