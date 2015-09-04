//
//  LuaReturn.h
//  LuaState
//
//  Created by Simon Mikuda on 18/03/14.
//
//  See LICENSE and README.md files

#pragma once

namespace lua {
    
    //////////////////////////////////////////////////////////////////////////////////////////////////
    namespace stack {
        
        
        //////////////////////////////////////////////////////////////////////////////////////////////////
        template<std::size_t I, typename ... Ts>
        class Pop {
            
            friend class lua::Value;
            
            /// Function get single value from lua stack
            template<typename T>
            static inline T readValue(const std::shared_ptr<lua_State>& luaState,
                                      detail::DeallocQueue* deallocQueue,
                                      int stackTop)
            {
                return std::move(lua::Value(luaState, deallocQueue, stackTop - 1));
            }
            
            /// Function creates indexes for mutli values and get them from stack
            template<std::size_t... Is>
            static std::tuple<Ts...> unpackMultiValues(const std::shared_ptr<lua_State>& luaState,
                                                       detail::DeallocQueue* deallocQueue,
                                                       int stackTop,
                                                       traits::indexes<Is...>)
            {
                return std::make_tuple(readValue<Ts>(luaState, deallocQueue, Is + stackTop)...);
            }
            
        public:

            /// Function get multiple return values from lua stack
            static std::tuple<Ts...> getMultiValues(const std::shared_ptr<lua_State>& luaState,
                                                    detail::DeallocQueue* deallocQueue,
                                                    int stackTop)
            {
                return unpackMultiValues(luaState, deallocQueue, stackTop, typename traits::indexes_builder<I>::index());
            }
        };
        
        /// Function expects that number of elements in tuple and number of pushed values in stack are same. Applications takes care of this requirement by popping overlapping values before calling this function
        template<typename ... Ts>
        inline std::tuple<Ts...> get_and_pop(const std::shared_ptr<lua_State>& luaState,
                                             detail::DeallocQueue* deallocQueue,
                                             int stackTop)
        {
            return Pop<sizeof...(Ts), Ts...>::getMultiValues(luaState, deallocQueue, stackTop);
        }
        
        
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////
    /// Class for automaticly cas lua::Function instance to multiple return values with lua::tie
	template <typename ... Ts>
	class Return
    {
        /// Return values
	    std::tuple<Ts&...> _tuple;
        
	public:
        
        /// Constructs class with given arguments
        ///
        /// @param args    Return values
	    Return(Ts&... args)
        : _tuple(args...) {}
        
        /// Operator sets values to std::tuple
        ///
        /// @param function     Function being called
	    void operator= (const Value& value) {
            
            int requiredValues = sizeof...(Ts) < value._pushedValues ? sizeof...(Ts) : value._pushedValues;
            
            // When there are more returned values than variables in tuple, we will clear values that are not needed
            if (requiredValues < (value._groupedValues + 1)) {
                
                int currentStackTop = stack::top(value._luaState);
                
                // We will check if we haven't pushed some other new lua::Value to stack
                if (value._stackTop + value._pushedValues == currentStackTop)
                    stack::settop(value._luaState, value._stackTop + requiredValues);
                else
                    value._deallocQueue->push(detail::DeallocStackItem(value._stackTop, value._pushedValues));
            }
            
            // We will take pushed values and distribute them to returned lua::Values
            value._pushedValues = 0;
            
            _tuple = stack::get_and_pop<typename std::remove_reference<Ts>::type...>(value._luaState, value._deallocQueue, value._stackTop + 1);
	    }

	};
    
    /// Use this function when you want to retrieve multiple return values from lua::Function
    template <typename ... Ts>
    Return<Ts&...> tie(Ts&... args) {
        return Return<Ts&...>(args...);
    }
}
