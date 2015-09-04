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

#include "asyncurl.h"

#include "sysutils.h"

#define NOMINMAX
#include <algorithm>
#include <stdexcept>

#include <curl/curl.h>

namespace webstor
{

namespace internal
{

//////////////////////////////////////////////////////////////////////////////
// AsyncState -- async state potion of cURL object.

struct AsyncState
{
                    AsyncState();

    void            setCompleted() { completedEvent.set(); }
    bool            isCompleted() { return completedEvent.wait( 0 ); }

    static void         setToCurl( CURL *curl, AsyncState *as );  // nofail
    static AsyncState * getFromCurl( CURL *curl );  // nofail

    EventSync       completedEvent;
    CURLcode        opResult;

    SocketHandle    socket;
    AsyncLoop *     asyncLoop;

#ifdef PERF
    UInt64          creationTimestamp;
#endif
};

inline
AsyncState::AsyncState()
    : opResult( CURLE_OK )
    , socket( 0 )
    , asyncLoop( 0 )
{ 
    completedEvent.set();

#ifdef PERF
    creationTimestamp = timeElapsed();
#endif
}

inline void
AsyncState::setToCurl( CURL *curl, AsyncState *as )  // nofail
{
    dbgAssert( curl );
    dbgVerify( curl_easy_setopt( curl, CURLOPT_PRIVATE, as ) == CURLE_OK ); 
}

inline AsyncState *
AsyncState::getFromCurl( CURL *curl )  // nofail
{
    dbgAssert( curl );

    AsyncState *as = 0;
    dbgVerify( curl_easy_getinfo( curl, CURLINFO_PRIVATE, &as ) == CURLE_OK );
    return as; 
}

//////////////////////////////////////////////////////////////////////////////
// AsyncLoop -- async cURL-multi object.

#define curl_multi_setopt_checked( handle, option, ... ) dbgVerify( curl_multi_setopt( handle, option, __VA_ARGS__ ) == CURLM_OK )

#define curl_multi_assign_checked( handle, socket, data ) dbgVerify( curl_multi_assign( handle, socket, data ) == CURLM_OK )

class AsyncLoop
{
public:
                    AsyncLoop();
    static void     destroy( AsyncLoop *head );

    static void     pendOp( AsyncLoop *head, CURL *request, size_t connectionsPerThread );
    void            cancelOp( CURL *request );  // nofail
private:
    enum { c_maxSocketTimeout = 3000, c_interruptOnlyTimeout = -1 };

                    AsyncLoop( const AsyncLoop & );  // forbidden
    AsyncLoop &     operator=( const AsyncLoop & );  // forbidden

                    ~AsyncLoop();
    bool            pendOp( CURL *request, size_t connectionsPerThread,
                        size_t *totalRequest );

    void            asyncLoop();
    static TaskResult TASKAPI asyncLoopTask( void *arg );

    void            handlePendingRequests();
    void            addNewRequests();
    void            removeCanceledRequests();
    void            removeCompletedRequests();

    void            addSocket( AsyncState *asyncState, curl_socket_t socket, int what ); // nofail
    void            removeSocket( AsyncState *asyncState );  // nofail

    void            executeSocketAction( SocketHandle socket, SocketActionMask actionMask = 0 );  // nofail

    // Curl callbacks.

    static int      handleAddRemoveSocket( CURL *curl, curl_socket_t socket, 
                        int what, void *ctx, void *socketData ); // nofail
    static int      handleTimeout( CURLM *multi, long msTimeout, void *ctx ); // nofail

    // Curl multi-handle.

    CURLM          *m_multiCurl;

    // Background thread control.

    bool            m_shutdown;
    TaskCtrl        m_asyncLoopTaskCtrl;

    // Timeout recommended by curl.

    UInt32          m_socketActionTimeout;

    // A pool of active sockets provided by curl through the handleAddRemoveSocket callback.
    // The field is used by asyncLoop thread only.
    // We don't have any synchronization for this field.

    SocketPool      m_socketPool;

    ExLockSync      m_lock;

    // The next asyncLoop item. Needed to handle more requests than one asyncLoop
    // can process. 
    // The head asyncLoop is always created by ConnectionAsyncManager.
    // Subsequent asyncLoops get created on-demand and they never destroy. In other words,
    // if 'next' is not NULL, it will stay not NULL and won't change.

    AsyncLoop *volatile     m_next;    // all write access must go through the m_lock

    // Pending new and canceled request. Can be added by any thread but removed by asyncLoop thread only.
    // (all access must be synchronized through the m_lock.)

    std::vector< CURL * >   m_pendingRequests; 
    std::vector< CURL * >   m_canceledRequests;

    // Number of running requests (number of easy handles in the multi-handle).
    // It's equal or greater than the number of sockets in the m_socketPool.
    // The field is modified by the asyncLoop thread only after easy handle is
    // added/removed to/from the multi-handle.

    volatile size_t m_runningRequestCount;

    // A flag indicating if there are pending new or canceled requests.

    volatile bool   m_hasPending;
};

static void
raiseIfError( CURLMcode multiCurlCode )
{
    dbgAssert( multiCurlCode != CURLM_CALL_MULTI_PERFORM );  // this is supposed to be obsolete error

    if( multiCurlCode == CURLM_OUT_OF_MEMORY )
    {
        throw std::bad_alloc();
    }

    if( multiCurlCode != CURLM_OK )
    {
        std::string msg( "curl: " );
        msg.append( curl_multi_strerror( multiCurlCode ) );
        throw std::runtime_error( msg );
    }
}

AsyncLoop::AsyncLoop()
    : m_multiCurl( NULL )
    , m_shutdown( false )
    , m_socketActionTimeout( c_maxSocketTimeout )
    , m_next ( NULL )
    , m_runningRequestCount( 0 )
    , m_hasPending( false )
{
    // Allocate a multi-handle.

    m_multiCurl = curl_multi_init();

    if( !m_multiCurl )
    {
        throw std::bad_alloc();
    }

    curl_multi_setopt_checked( m_multiCurl, CURLMOPT_SOCKETFUNCTION, handleAddRemoveSocket );
    curl_multi_setopt_checked( m_multiCurl, CURLMOPT_SOCKETDATA, this );
    curl_multi_setopt_checked( m_multiCurl, CURLMOPT_TIMERFUNCTION, handleTimeout );
    curl_multi_setopt_checked( m_multiCurl, CURLMOPT_TIMERDATA, this );

    // Start the background task.

    try
    {
        taskStartAsync( &asyncLoopTask, this, &m_asyncLoopTaskCtrl );
    }
    catch( ... )
    {
        dbgVerify( curl_multi_cleanup( m_multiCurl ) == CURLM_OK );
        throw;
    }

    LOG_TRACE( "create AsyncLoop: asyncLoop=0x%llx", ( UInt64 )this );
}

AsyncLoop::~AsyncLoop()
{
    LOG_TRACE( "destroy AsyncLoop: asyncLoop=0x%llx", ( UInt64 )this );

    // Shutdown the background thread.

    m_shutdown = true;
    m_socketPool.signal();   // nofail
    m_asyncLoopTaskCtrl.wait();     // nofail

    // Lifetime of AsyncMan should be greater than any async request managed through
    // this AsyncMan instance.
    // In other words, all async requests should be completed/canceled
    // (or connection deleted)
    // before AsyncMan can be destroyed.

    dbgAssert( m_runningRequestCount == 0 );

    // Release the multi-handle.

    dbgVerify( curl_multi_cleanup( m_multiCurl ) == CURLM_OK );
}

void
AsyncLoop::destroy( AsyncLoop *head ) // nofail
{
    while( head )
    {
        AsyncLoop *next = head->m_next;
        delete head;
        head = next;
    } 
}

TaskResult TASKAPI
AsyncLoop::asyncLoopTask( void *arg )
{
    dbgAssert( arg );
    static_cast< AsyncLoop * >( arg )->asyncLoop();
    return 0;
}

void
AsyncLoop::asyncLoop()
{
    // This runs as an asynchronous task.

    dbgAssert( m_multiCurl );

    SocketActions socketActions;

    while( !m_shutdown )
    {
        try
        {
            if( m_hasPending )
            {
                // Add new and remove canceled requests.

                handlePendingRequests();
                dbgAssert( m_runningRequestCount >= m_socketPool.size() );
            }

            size_t socketCount =  m_socketPool.size();

            if( m_runningRequestCount > socketCount )
            {
                // We have added more requests to the multi-handle than the number of sockets
                // curl reported back to us yet. Execute 'timeout' action.

                executeSocketAction( INVALID_SOCKET_HANDLE );
            }
            else
            {
                // Wait for any activity.

                socketActions.reserve( socketCount );

                if( m_socketPool.wait( m_socketActionTimeout, c_interruptOnlyTimeout, &socketActions ) )
                {
                    // Some activity (or interrupt) has been detected, handle it.

                    for( SocketActions::const_iterator it = socketActions.begin();
                        it != socketActions.end(); ++it )
                    {
                        executeSocketAction( it->first, it->second );
                    }
                }
                else
                {
                    // No activity, go through all sockets and check their status.
                    // Note: curl_multi_socket_all(..) is deprecated but it doesn't seem
                    // there is another way to check all sockets. Without this call some sockets
                    // may stuck for very long.

                    int stillRunning = 0;
                    CURLMcode multiCurlCode = curl_multi_socket_all( m_multiCurl, &stillRunning );
                    raiseIfError( multiCurlCode );
                }
            }

            // Remove completed requests.

            removeCompletedRequests();
            dbgAssert( m_runningRequestCount >= m_socketPool.size() );
        }
        catch( ... )
        {
            // Continue to work no matter what.

            handleBackgroundError();
            taskSleep( 3000 );
        }
    }
}

void
AsyncLoop::executeSocketAction( SocketHandle socket, SocketActionMask actionMask )  // nofail
{
    CASSERT( SA_POLL_IN == CURL_CSELECT_IN );
    CASSERT( SA_POLL_OUT == CURL_CSELECT_OUT );
    CASSERT( SA_POLL_ERR == CURL_CSELECT_ERR );
    CASSERT( sizeof( curl_socket_t ) == sizeof( SocketHandle ) );

    try
    {
        int stillRunning = 0;
        CURLMcode multiCurlCode =
            curl_multi_socket_action( m_multiCurl, ( curl_socket_t ) socket, actionMask, &stillRunning );

        raiseIfError( multiCurlCode );
    }
    catch( ... )
    {
        // We cannot fail this method, handle the error and continue.

        handleBackgroundError();
    }
}

int
AsyncLoop::handleAddRemoveSocket( CURL *curl, curl_socket_t socket, int what, void *ctx, void *socketData )  // nofail
{
    AsyncLoop *const asyncLoop = static_cast< AsyncLoop * >( ctx );
    dbgAssert( asyncLoop );
    AsyncState *const asyncState = AsyncState::getFromCurl( curl );  // nofail
    dbgAssert( asyncState );

    if( what == CURL_POLL_REMOVE )
    {
        asyncLoop->removeSocket( asyncState );  // nofail

        // Do not try to do anything with the socket here because it may be invalid.
    } 
    else
    {
        asyncLoop->addSocket( asyncState, socket, what );  // nofail
    }

    return 0;
}

int
AsyncLoop::handleTimeout( CURLM *multi, long msTimeout, void *ctx ) // nofail
{
    AsyncLoop *const asyncLoop = static_cast< AsyncLoop * >( ctx );
    dbgAssert( asyncLoop );

    if( msTimeout == 0 )
    {
        // Execute 'timeout' action.

        asyncLoop->executeSocketAction( INVALID_SOCKET_HANDLE );  // nofail
        asyncLoop->m_socketActionTimeout = c_maxSocketTimeout;
    }
    else
    {
        asyncLoop->m_socketActionTimeout = std::min( static_cast< UInt32 >( msTimeout ),  // this handles -1 as well
            static_cast< UInt32 >( c_maxSocketTimeout ) );
    }

    return 0;
}

void
AsyncLoop::handlePendingRequests() 
{
    m_lock.claimLock();  
    ScopedExLock lock( &m_lock );

    // Check if there are any new requests, add them to the multi-handle.

    addNewRequests();

    // Check if there are any canceled requests, remove them from the multi-handle.

    removeCanceledRequests();

    m_hasPending = false;
}

void
AsyncLoop::addNewRequests() 
{
    dbgAssert( m_lock.dbgHoldLock() );

    CURLMcode multiCurlCode = CURLM_OK;

    // Reserve space in the socketList to ensure nofail in addSocket(..).

    m_socketPool.reserve( m_runningRequestCount + m_pendingRequests.size() );  // can throw std::bad_alloc.

    // Add pending requests.

    if( !m_pendingRequests.empty() )
    {
        for( size_t i = 0; i < m_pendingRequests.size(); ++i )
        {
            CURL *const request = m_pendingRequests[ i ];
            dbgAssert( request );
            AsyncState *const asyncState = AsyncState::getFromCurl( request );
            dbgAssert( asyncState );
            dbgAssert( !asyncState->isCompleted() );

            m_pendingRequests[ i ] = NULL;

            if( ( multiCurlCode = curl_multi_add_handle( m_multiCurl, request ) ) == CURLM_OK )
            {
                m_runningRequestCount++;

#ifdef PERF
                LOG_TRACE( "request enqueueing lag: request=0x%llx, runningCount=%llu, asyncLoop=0x%llx, elapsed=%llu", 
                    ( UInt64 )request, m_runningRequestCount, ( UInt64 )this,  
                    timeElapsed() - asyncState->creationTimestamp );
#endif
            }
            else
            {
                dbgAssert( multiCurlCode == CURLM_OUT_OF_MEMORY );
                asyncState->opResult = CURLE_OUT_OF_MEMORY;
                asyncState->setCompleted();
            }
        }

        m_pendingRequests.clear();
    }
}

void
AsyncLoop::removeCanceledRequests() 
{
    dbgAssert( m_lock.dbgHoldLock() );

    if( !m_canceledRequests.empty() )
    {
        for( size_t i = 0; i < m_canceledRequests.size(); ++i )
        {
            CURL *const request = m_canceledRequests[ i ];
            dbgAssert( request );
            AsyncState *const asyncState = AsyncState::getFromCurl( request );
            dbgAssert( asyncState );

            m_canceledRequests[ i ] = NULL;

            if( !asyncState->isCompleted() )
            {
                dbgVerify( m_socketPool.remove( asyncState->socket ) );
                dbgVerify( curl_multi_remove_handle( m_multiCurl, request ) == CURLM_OK );
                dbgAssert( m_runningRequestCount );
                m_runningRequestCount--;
                asyncState->setCompleted();
            }
        }

        m_canceledRequests.clear();
    }
}

void
AsyncLoop::removeSocket( AsyncState *asyncState )  // nofail
{
    if( asyncState )
        m_socketPool.remove( asyncState->socket );  // nofail
}

void
AsyncLoop::addSocket( AsyncState *asyncState, curl_socket_t socket, int what ) // nofail
{
    dbgAssert( asyncState );
    dbgAssert( !asyncState->isCompleted() );
    CASSERT( SA_POLL_IN == CURL_POLL_IN );
    CASSERT( SA_POLL_OUT == CURL_POLL_OUT );

    // Assign the socket to the socketReady signal.

    CASSERT( sizeof( curl_socket_t ) == sizeof( SocketHandle ) );
    asyncState->socket = ( SocketHandle )( socket );
    m_socketPool.add( asyncState->socket, what );  // nofail 
}

void
AsyncLoop::removeCompletedRequests() 
{
    int left = 0;

    while( CURLMsg *const msg = curl_multi_info_read( m_multiCurl, &left ) )
    {
        if( msg->msg == CURLMSG_DONE ) 
        {
            CURL *curl = msg->easy_handle;
            CURLcode curlCode = msg->data.result;

            // Note: msg points to an internal record which doesn't survive 
            // curl_multi_remove_handle(..) call,
            // so don't access msg after this line!

            dbgVerify( curl_multi_remove_handle( m_multiCurl, curl ) == CURLM_OK );
            dbgAssert( m_runningRequestCount );

            m_runningRequestCount--;

            AsyncState *const asyncState = AsyncState::getFromCurl( curl );  // nofail
            dbgAssert( asyncState );

            removeSocket( asyncState );

            // Save if the request failed, the error will be raised by the thread that
            // calls completeXXX.

            asyncState->opResult = curlCode;

#ifdef PERF
            LOG_TRACE( "request completion time: request=0x%llx, runningCount=%llu, asyncLoop=0x%llx, elapsed=%llu", 
                ( UInt64 )curl, m_runningRequestCount, ( UInt64 )this,  
                timeElapsed() - asyncState->creationTimestamp );
#endif

            // Now tell everyone that the request has completed.

            asyncState->setCompleted();
        }
    }
}

void
AsyncLoop::pendOp( AsyncLoop *head, CURL *request, size_t connectionsPerThread )
{
    dbgAssert( head );
    dbgAssert( request );

    //
    // Algorithm:
    //
    // 1. if we have only 1 thread/asyncLoop:
    //    then if number of requests < m_connectionsPerThread => append to that thread
    //             else create a new thread/asyncLoop.
    // 2. if we have multiple threads/asyncLoops:
    //    then append to the first thread with number of requests < half of m_connectionsPerThread.
    //    if we could not find such thread then find the min number of requests across all threads.
    //    if min < m_connectionsPerThread => append to that thread else create a new thread/asyncLoop.

    while( true )
    {
        size_t totalRequest = 0;
        size_t minTotalRequest = 0;

        AsyncLoop *candidate = head;
        AsyncLoop *last = head;

        if( head->m_next )
        {
            // We have multiple asyncLoops.

            size_t half = std::max( ( size_t )1, connectionsPerThread / 2 );
            minTotalRequest = -1;

            for( AsyncLoop *cur = head; cur; cur = cur->m_next )
            {
                // Make sure we see memory pointed to by cur->m_next as
                // initalized.

                cpuMemLoadFence();

                // Try to append with limit set to 'half'.

                if( cur->AsyncLoop::pendOp( request, half, &totalRequest ) )
                {
                    return;
                }

                // The current asyncLoop has more than 'half' requests.

                if( totalRequest < minTotalRequest )
                {
                    // Remember the min.

                    minTotalRequest = totalRequest;
                    candidate = cur;
                }

                // Remember in case this is the last.

                last = cur;
            }
        }

        // Try to append to the head (if this is the only asyncLoop) or
        // to the found 'min' candidate (if there are multiple asyncLoops).

        dbgAssert( candidate );

        if( minTotalRequest < connectionsPerThread &&
            candidate->AsyncLoop::pendOp( request, connectionsPerThread, &totalRequest ) )
        {
            return;
        }

        // We don't have an asyncLoop that can accommodate a new request, create a new one.

        candidate = new AsyncLoop();

        {
            head->m_lock.claimLock();  // nofail
            ScopedExLock lock( &head->m_lock );

            // While we were waiting for the lock, another thread could create and add a new asyncLoop.
            // Find the real last item.

            for( ; last->m_next; last = last->m_next ) {}

            // Add the created asyncLoop.

            last->m_next = candidate; 
        }

        // Add the request to the created asyncLoop.

        if( candidate->AsyncLoop::pendOp( request, connectionsPerThread, &totalRequest ) )
        {
            return;
        }

        // ops, another thread managed to append its request(s) to the new asyncLoop we have just created.
        // Repeat, we should be more lucky the next time.
    }
}

bool
AsyncLoop::pendOp( CURL *request, size_t connectionsPerThread, size_t *totalRequest )
{
    dbgAssert( request );
    dbgAssert( totalRequest );

    {
        m_lock.claimLock();
        ScopedExLock lock( &m_lock );

        *totalRequest = m_runningRequestCount + m_pendingRequests.size();

        if( *totalRequest >= connectionsPerThread )
        {
            return false;
        }

        // Pre-allocate some space to avoid std::bad_alloc and ensure nofail in cancel(..).

        m_canceledRequests.reserve( *totalRequest + 1 );  // +1 one extra for the request being appended.

        // Append pending request.

        AsyncState *const asyncState = AsyncState::getFromCurl( request );  // nofail
        dbgAssert( asyncState );

        m_pendingRequests.push_back( request );  // can throw std::bad_alloc
        asyncState->completedEvent.reset();  // nofail
        asyncState->opResult = CURLE_BAD_FUNCTION_ARGUMENT;
        asyncState->asyncLoop = this;
        ( *totalRequest )++;

        m_hasPending = true;
    }

    m_socketPool.signal(); // nofail
    return true;
}

void
AsyncLoop::cancelOp( CURL *request ) // nofail
{
    dbgAssert( request );
    AsyncState *const asyncState = AsyncState::getFromCurl( request );  // nofail
    dbgAssert( asyncState );

    if( !asyncState->isCompleted() )
    {
        // Append cancellation.
        {
            m_lock.claimLock();  
            ScopedExLock lock( &m_lock );

            dbgAssert( m_canceledRequests.capacity() > m_pendingRequests.size() );
            m_canceledRequests.push_back( request ); // nofail because it has enough capacity reserved by pendOp(..)

            m_hasPending = true;
        }

        m_socketPool.signal(); // nofail

        // Wait for the complete event.

        asyncState->completedEvent.wait(); // nofail
    }
}

//////////////////////////////////////////////////////////////////////////////
// AsyncCurl -- cURL extended with async functionality.

AsyncCurl::AsyncCurl()
{
    m_asyncState = new AsyncState;
    m_curl = curl_easy_init();

    if( !m_curl )
    {
        delete m_asyncState;
        throw std::bad_alloc();
    }
}

AsyncCurl::~AsyncCurl()
{
    cancelOp();  // nofail

    delete m_asyncState;
    curl_easy_cleanup( m_curl );
}

void
AsyncCurl::pendOp( AsyncMan *opMan )
{
    dbgAssert( opMan );
    dbgAssert( !m_asyncState->asyncLoop );
    dbgAssert( !AsyncState::getFromCurl( m_curl ) || AsyncState::getFromCurl( m_curl ) == m_asyncState );

    AsyncState::setToCurl( m_curl, m_asyncState );
    AsyncLoop::pendOp( opMan->head(), m_curl, opMan->connectionsPerThread() );
    dbgAssert( m_asyncState->asyncLoop );
}

void
AsyncCurl::completeOp()  // nofail
{
    m_asyncState->completedEvent.wait();  // nofail
    m_asyncState->asyncLoop = 0;
    AsyncState::setToCurl( m_curl, 0 );  // nofail
}

void
AsyncCurl::cancelOp()  // nofail
{
    if( m_asyncState->asyncLoop )
        m_asyncState->asyncLoop->cancelOp( m_curl );

    m_asyncState->asyncLoop = 0;
    AsyncState::setToCurl( m_curl, 0 );  // nofail
}

bool
AsyncCurl::isOpCompleted() const  // nofail
{
    return m_asyncState->isCompleted();  // nofail
}

int
AsyncCurl::opResult() const  // nofail, returns CURLcode
{
    return m_asyncState->opResult;
}

EventSync *     
AsyncCurl::completedEvent() const
{
    return &m_asyncState->completedEvent;
}

}  // namespace internal

using namespace internal;

//////////////////////////////////////////////////////////////////////////////
// AsyncCurlOpMan -- manager for async cURL operations.

AsyncMan::AsyncMan( size_t connectionsPerThread )
    : m_head( new AsyncLoop )
    , m_connectionsPerThread( connectionsPerThread + !connectionsPerThread )
{
    if( m_connectionsPerThread > c_cMaxConnectionsPerThread )
        m_connectionsPerThread = c_cMaxConnectionsPerThread;
}

AsyncMan::~AsyncMan()
{
    AsyncLoop::destroy( m_head );
}

//////////////////////////////////////////////////////////////////////////////
// Background error handling. 

static BackgroundErrHandler *s_backgroundErrHandler = 0;

void
internal::handleBackgroundError()  // nofail
{
    // This function must be called from inside of a catch( ... ).

    if( s_backgroundErrHandler )
    {
        ( *s_backgroundErrHandler )();  // must be nofail
    }
    else
    {
        dbgPanicSz( "BUG: background error handler is unset!!!" );
    }
}

void
setBackgroundErrHandler( BackgroundErrHandler *eh )
{
    s_backgroundErrHandler = eh;
}

}  // namespace webstor
