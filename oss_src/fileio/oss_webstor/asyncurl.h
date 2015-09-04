#ifndef INCLUDED_ASYNCURL_H
#define INCLUDED_ASYNCURL_H

//////////////////////////////////////////////////////////////////////////////
// Copyright (c) 2011-2013, OblakSoft LLC.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
// http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// 
//
// Authors: Maxim Mazeev <mazeev@hotmail.com>
//          Artem Livshits <artem.livshits@gmail.com>

//////////////////////////////////////////////////////////////////////////////
// Async functionality for cURL.
//////////////////////////////////////////////////////////////////////////////

#include <stddef.h>

namespace webstor
{

class AsyncMan;

namespace internal
{

typedef void CURL;

struct AsyncState;
class AsyncLoop;
class EventSync;

//////////////////////////////////////////////////////////////////////////////
///@brief INTERNAL: AsyncCurl -- cURL extended with async functionality.
///@remarks WARNING: async operations use CURLOPT_PRIVATE option, so it must not
/// be used for other purposes while async functionality is used.

class AsyncCurl
{
public:
                    AsyncCurl();
                    ~AsyncCurl();

    operator        CURL *() const { return m_curl; }

    void            pendOp( AsyncMan *opMan );

    void            completeOp();  // nofail
    void            cancelOp();  // nofail

    bool            isOpCompleted() const;  // nofail
    int             opResult() const;  // nofail, returns CURLcode

    EventSync *     completedEvent() const;

private:
                    AsyncCurl( const AsyncCurl & );  // forbidden
    AsyncCurl &     operator=( const AsyncCurl & );  // forbidden

    CURL *          m_curl;
    AsyncState *    m_asyncState;
};


void 
handleBackgroundError();  // nofail

}  


//////////////////////////////////////////////////////////////////////////////
///@brief AsyncMan -- manager for async operations.
///@details An instance of this class is needed to initiate an async cURL operation.
/// Generally, one instance per application is needed.
///
///@remarks Lifetime of the connection manager must be greater than any active async
/// cURL operation initiated with this manager.
///
///@remarks Thread-safety: the object is thread-safe and can be used by multiple
/// connections from multiple threads.
///

class AsyncMan
{
public:

    /// Default number of connections per thread.

    enum { c_cMaxConnectionsPerThread = 32 };

    ///@brief Constructs a new instance of AsyncMan. 
    ///@details </b>connectionsPerThread</b> specifies
    /// how many connections can be handled by a single thread.
    /// If more that <b>connectionsPerThread</b> async operation is started at
    /// the same time, the class can spawn extra threads as needed.

    explicit        AsyncMan( size_t connectionsPerThread = c_cMaxConnectionsPerThread );

    /// Terminates AsyncMan.

                    ~AsyncMan();

    /// Number of connectinos per thread.

    size_t                  connectionsPerThread() const { return m_connectionsPerThread; }

public:
    internal::AsyncLoop *   head() const { return m_head; }

private:
                    AsyncMan( const AsyncMan & );  // forbidden
    AsyncMan& operator=( const AsyncMan & );  // forbidden

    internal::AsyncLoop *   m_head;
    size_t                  m_connectionsPerThread;
};

//////////////////////////////////////////////////////////////////////////////
// Background error handling support.

/// Background error handling callback.

typedef void ( BackgroundErrHandler )();

/// Set this to be able to log and recover from errors happened on
/// a background thread while executing an async request.

void
setBackgroundErrHandler( BackgroundErrHandler *eh );

}  // namespace webstor

#endif // !INCLUDED_ASYNCURL_H
