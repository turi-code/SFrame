//
//  Traits.h
//  LuaState
//
//  Created by Simon Mikuda on 22/03/14.
//
//  See LICENSE and README.md files//

#pragma once

namespace lua { namespace traits {
    
    //////////////////////////////////////////////////////////////////////////////////////////////////
    
    template <typename T>
    struct function_traits
    : public function_traits<decltype(&T::operator())>
    {};
    
    template <typename ClassType, typename ReturnType, typename... Args>
    struct function_traits<ReturnType(ClassType::*)(Args...) const>
    {
        enum { arity = sizeof...(Args) };
        
        typedef ReturnType ResultType;
        typedef std::function<ReturnType(Args...)> Function;
        
        template <size_t i>
        struct Arg {
            typedef typename std::tuple_element<i, std::tuple<Args...>>::type type;
        };
    };

    //////////////////////////////////////////////////////////////////////////////////////////////////
    
    template<size_t...> struct index_tuple{};
    
    template<size_t I, typename IndexTuple, typename ... Types>
    struct make_indexes_impl;
    
    template<size_t I, size_t... Indexes, typename T, typename ... Types>
    struct make_indexes_impl<I, index_tuple<Indexes...>, T, Types...>
    {
        typedef typename make_indexes_impl<I + 1, index_tuple<Indexes..., I>, Types...>::type type;
    };
    
    template<size_t I, size_t... Indexes>
    struct make_indexes_impl<I, index_tuple<Indexes...> >
    {
        typedef index_tuple<Indexes...> type;
    };
    
    template<typename ... Types>
    struct make_indexes : make_indexes_impl<0, index_tuple<>, Types...>
    {};
    
    //////////////////////////////////////////////////////////////////////////////////////////////////
    
    template<class Ret, class... Args, size_t... Indexes >
    Ret apply_helper(std::function<Ret(Args...)> pf, index_tuple< Indexes... >, std::tuple<Args...>&& tup)
    {
        return pf( std::forward<Args>( std::get<Indexes>(tup))... );
    }
    
    template<class Ret, class ... Args>
    Ret apply(std::function<Ret(Args...)> pf, const std::tuple<Args...>&  tup)
    {
        return apply_helper(pf, typename make_indexes<Args...>::type(), std::tuple<Args...>(tup));
    }
    
    template<class Ret, class ... Args>
    Ret apply(std::function<Ret(Args...)> pf, std::tuple<Args...>&&  tup)
    {
        return apply_helper(pf, typename make_indexes<Args...>::type(), std::forward<std::tuple<Args...>>(tup));
    }
    
    template<class ... Args>
    void apply(std::function<void(Args...)> pf, const std::tuple<Args...>&  tup)
    {
        apply_helper(pf, typename make_indexes<Args...>::type(), std::tuple<Args...>(tup));
    }
    
    template<class ... Args>
    void apply(std::function<void(Args...)> pf, std::tuple<Args...>&&  tup)
    {
        apply_helper(pf, typename make_indexes<Args...>::type(), std::forward<std::tuple<Args...>>(tup));
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////
    
    template <std::size_t... Is>
    struct indexes {};
    
    template <std::size_t N, std::size_t... Is>
    struct indexes_builder : indexes_builder<N-1, N-1, Is...> {};
    
    template <std::size_t... Is>
    struct indexes_builder<0, Is...> {
        using index = indexes<Is...>;
    };
}}
