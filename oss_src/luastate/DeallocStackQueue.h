//
//  DeallocStackQueue.h
//  LuaState
//
//  Created by Simon Mikuda on 06/05/14.
//
//

#pragma once

#include <queue>

namespace lua { namespace detail {
        
    //////////////////////////////////////////////////////////////////////////////////////////////////
    struct DeallocStackItem {
        int stackCap;
        int numElements;
        
        DeallocStackItem(int stackTop, int numElements) : stackCap(stackTop + numElements), numElements(numElements) {}
    };
    
    //////////////////////////////////////////////////////////////////////////////////////////////////
    struct DeallocStackComparison {
        bool operator() (const DeallocStackItem& lhs, const DeallocStackItem& rhs) const {
            return lhs.stackCap < rhs.stackCap;
        }
    };
    
    //////////////////////////////////////////////////////////////////////////////////////////////////
    typedef std::priority_queue<DeallocStackItem, std::vector<DeallocStackItem>, DeallocStackComparison> DeallocQueue;
   
    static DeallocQueue* DEALLOC_QUEUE;
} }
