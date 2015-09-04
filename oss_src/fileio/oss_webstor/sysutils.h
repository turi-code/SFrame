#ifndef INCLUDED_SYSUTIL_H
#define INCLUDED_SYSUTIL_H

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
// System level classes and common utils.
//////////////////////////////////////////////////////////////////////////////

#include <stddef.h>  // needed for __WORDSIZE and size_t 
#include <string>
#include <vector>
#include <set>
#include <parallel/pthread_h.h>
namespace webstor
{

namespace internal
{

//////////////////////////////////////////////////////////////////////////////
// Debugging support.

#ifdef DEBUG

typedef bool ( dbgShowAssertFunc )( const char *file, int line, const char *msg, bool *ignoreAll );
extern dbgShowAssertFunc *g_dbgShowAssertFunc_;

#ifdef _MSC_VER
#define dbgBreak_ __debugbreak()
#elif defined( __GNUC__ )
extern volatile int g_fakeGlobalForDbgBreak_;
#define dbgBreak_ do { asm __volatile__ ( "int3" ); g_fakeGlobalForDbgBreak_ = 0; } while( 0 )
#else
#define dbgBreak_
#endif

#define dbgAssertSz( cond, sz ) \
    do \
    { \
        static bool s_ignoreAll = false; \
        if( !s_ignoreAll && !( cond ) && \
            ( !g_dbgShowAssertFunc_ || ( *g_dbgShowAssertFunc_ )( __FILE__, __LINE__, sz, &s_ignoreAll ) ) ) \
            dbgBreak_; \
    } while( 0 )

#define dbgVerify( a ) dbgAssert( a )
#define ifDebug( a ) a

#else // !DEBUG

#define dbgAssertSz( cond, sz ) do {} while( 0 )
#define dbgVerify( a ) do { if( !( a ) ) {}; } while( 0 )
#define ifDebug( a )

#endif // !DEBUG

#define dbgAssert( a ) dbgAssertSz( a, "Assertion failed: " #a )
#define dbgPanicSz( a ) dbgAssertSz( 0, a )

// This needs to be a macro so that short-circuiting happens
// (i.e. 'b' isn't evaluated unless 'a' is true).

#define implies( a, b ) ( !( a ) || ( b ) )

//////////////////////////////////////////////////////////////////////////////
// Compile-time asserts.

#define CASSERT( a ) typedef int compile_time_assert_failed__[ ( a ) ? 1 : -1 ]

//////////////////////////////////////////////////////////////////////////////
// Tracing support.

// #define WEBSTOR_ENABLE_DBG_TRACING

#ifdef WEBSTOR_ENABLE_DBG_TRACING
void 
logTrace( const char *fmt, ... );

#define LOG_TRACE( fmt, ... ) logTrace( "%llu " fmt "\n", timeElapsed(), __VA_ARGS__ )
#else  // !WEBSTOR_ENABLE_DBG_TRACING
#define LOG_TRACE( fmt, ... )
#endif  // !WEBSTOR_ENABLE_DBG_TRACING

//////////////////////////////////////////////////////////////////////////////
// Integer types.

typedef signed int Int32;
typedef unsigned int UInt32;

typedef signed long long Int64;
typedef unsigned long long UInt64;

CASSERT( sizeof( UInt64 ) == sizeof( UInt32 ) * 2 );

//////////////////////////////////////////////////////////////////////////////
// Built-in array dimension operator and other helper macros.

template < class T, size_t N > char ( *dimensionOfHelper_( T( & )[ N ] ) )[ N ];
#define dimensionOf( a ) sizeof( *webstor::internal::dimensionOfHelper_( a ) )

#define STRING_WITH_LEN( str )    ( str ), dimensionOf( str ) - 1

struct StringWithLen
{
    const char *    str;
    size_t          len;
};

//////////////////////////////////////////////////////////////////////////////
// Scoped resource (similar to auto_ptr/unique_ptr but for any resource that
// has a custom deleter).

template < class Type, class Deleter, UInt64 emptyValue = 0 >
class auto_scope 
{
public:
    explicit        auto_scope( Type o = ( Type )emptyValue ) : m_obj( o ) {}
                    ~auto_scope() { if( m_obj != ( Type )emptyValue ) Deleter::free( m_obj ); }

    Type            release() { Type o = m_obj; m_obj = ( Type )emptyValue; return o; }
    void            reset( Type o ) { if( m_obj != ( Type )emptyValue ) Deleter::free( m_obj ); m_obj = o; }

    operator        Type() const { return m_obj; }
    Type            operator->() const { dbgAssert( !empty() ); return m_obj; }

    bool            empty() const { return m_obj == ( Type )emptyValue; }

    Type *          unsafeAddress() { dbgAssert( empty() ); return &m_obj; }

private:
                    auto_scope( const auto_scope & );
    auto_scope&     operator=( const auto_scope & );

    Type            m_obj;
};

//////////////////////////////////////////////////////////////////////////////
// Memory fences (barriers).

#ifdef _MSC_VER
extern "C" void _mm_lfence(void);
extern "C" void _mm_mfence(void);
#pragma intrinsic (_mm_lfence)
#pragma intrinsic (_mm_mfence)

inline void
cpuMemLoadFence()
{
    _mm_lfence();
}

inline void
cpuMemStoreFence()
{
    _mm_mfence();
}

inline void
cpuMemFullFence()
{
    _mm_mfence();
}
#else // !_MSC_VER

inline void
cpuMemLoadFence()
{
    __sync_synchronize();
}

inline void
cpuMemStoreFence()
{
    __sync_synchronize();
}

inline void
cpuMemFullFence()
{
    __sync_synchronize();
}
#endif // !_MSC_VER


#ifndef _WIN32
class EventSync;

class EventWaiter
{
  friend class EventSync;
  EventWaiter();
public:
private:
  pthread_mutex_t lock;
  pthread_cond_t condvar;
};
#endif

//////////////////////////////////////////////////////////////////////////////
// EventSync -- event synchronization primitive.

class EventSync
{
    friend class SocketPool;
    friend class EventWaiter;

public:
    enum { c_infinite = -1 };

    EventSync( bool initialState = false );
    ~EventSync();

    void            set();  // nofail
    void            reset();  // nofail
    bool            wait( UInt32 msTimeout = c_infinite ) const;  // nofail

    // Waits for any, returns -1 if timeout.
    // count must be <= c_maxEventCount.

    enum { c_maxEventCount = 64 };

    static int      waitAny( EventSync **events, size_t count, UInt32 msTimeout = c_infinite ); 

private:
    EventSync( const EventSync& );
    EventSync&      operator=( const EventSync& );
#ifdef _WIN32
    void           *m_handle;
#else

    void add_waiter(EventWaiter*) const;
    void remove_waiter(EventWaiter*) const;
    int get_state() const;

    mutable pthread_mutex_t lock;
    mutable std::set<EventWaiter*> waiters_list;
    int state;
#endif
};

//////////////////////////////////////////////////////////////////////////////
// ExLockSync -- exclusive lock.

class ExLockSync 
{
#ifdef _WIN32

    struct Data     // Stub for CRITICAL_SECTION
    {
        void *      reserved1_;
        UInt32      reserved2_[ 2 ];
        void *      reserved3_[ 3 ];
    };

#else  // !_WIN32

    union Data      // Stub for pthread_mutex_t
    {
        struct
        {
            int         reserved1_;
            unsigned    reserved2_;
            int         reserved3_;
#if __WORDSIZE == 64
            unsigned    reserved4_;
#endif
            int         reserved5_;
#if __WORDSIZE == 64
            int         reserved6_;
            void *      reserved7_;
#else  // __WORDSIZE != 64
            unsigned    reserved6_;

            __extension__ union
            {
                int     reserved7_;
                void *  reserved8_;
            };
#endif  // __WORDSIZE != 64
        } reserved9_;
#if __WORDSIZE == 64
        char        reserved10_[ 40 ];
#else
        char        reserved10_[ 24 ];
#endif
        long        reserved11_;

        pthread_mutex_t m;
    };

#endif  // !_WIN32
public:
                    ExLockSync();
                    ~ExLockSync();

    void            claimLock();  // nofail
    void            releaseLock();  // nofail

#ifdef DEBUG
    bool            dbgHoldLock() const { return m_lockOwner != 0; }
#endif

private:
                    ExLockSync( const ExLockSync & );  // forbidden
    ExLockSync &    operator=( const ExLockSync & );  // forbidden

    Data            m_data;

#ifdef DEBUG
    UInt64          m_lockOwner; 
#endif
};

//////////////////////////////////////////////////////////////////////////////
// ScopedExLock -- scoped exclusive lock.

struct ExLockDeleter
{
    static void     free( ExLockSync *pels ) { dbgAssert( pels ); pels->releaseLock(); }
};

typedef auto_scope< ExLockSync *, ExLockDeleter > ScopedExLock;

//////////////////////////////////////////////////////////////////////////////
// SocketPool -- a collection of sockets with interruptible wait.

#ifdef _WIN32
typedef void *SocketHandle;
#else
typedef int SocketHandle;
#endif

#define INVALID_SOCKET_HANDLE ( ( SocketHandle )( -1 ) )

enum SocketAction
{ 
    SA_POLL_IN = 1, 
    SA_POLL_OUT = 2,
    SA_POLL_ERR = 4
};

typedef UInt32 SocketActionMask;

typedef std::vector< std::pair< SocketHandle, SocketActionMask > > SocketActions;

struct SocketPoolState;

class SocketPool 
{
public:
                    SocketPool();
                    ~SocketPool();

    bool            add( SocketHandle socket, SocketActionMask actionMask );  // nofail
    bool            remove( SocketHandle socket );  // nofail
    void            reserve( size_t size );
    size_t          size() const;  // nofail

    void            signal();  // nofail
    bool            wait( UInt32 msTimeout, UInt32 msInterruptOnlyTimeout, SocketActions *socketActions );

private:
                    SocketPool( const SocketPool & );  // forbidden
    SocketPool &    operator=( const SocketPool & );  // forbidden

    SocketPoolState *   m_pool;
    EventSync           m_interrupt;
};

//////////////////////////////////////////////////////////////////////////////
// Socket tuning.

struct TcpKeepAliveParams
{
    // How long to stay in idle before sending keepalive probe, in milliseconds.

    int             probeStartTime;  

    // Delay between TCP keepalive probes, in milliseconds.

    int             probeIntervalTime; 

    // The number of unacknowledged probes to send before considering the connection dead.

    int             probeCount; 
};

// Enable/Disable TCP KeepAlive.

void
setTcpKeepAlive( SocketHandle socket, const TcpKeepAliveParams *params );

// Change Send/Receive buffer size.

void
setSocketBuffers( SocketHandle socket, UInt32 size );

//////////////////////////////////////////////////////////////////////////////
// TaskCtrl -- asynchronous task control.

#ifdef _WIN32
typedef void * TaskHandle;
#else
typedef pthread_t TaskHandle;
#endif

struct TaskCtrlDeleter
{
    static void     free( TaskHandle handle );
};

class TaskCtrl : public auto_scope< TaskHandle, TaskCtrlDeleter >
{
public:
    void            wait() const;  // nofail
};

//////////////////////////////////////////////////////////////////////////////
// Task utilities.

#ifdef _WIN32
typedef unsigned long TaskResult;
#define TASKAPI __stdcall
#else
typedef void * TaskResult;
#define TASKAPI
#endif

typedef UInt64 TaskId;

const TaskId c_invalidTaskId = 0;

typedef TaskResult ( TASKAPI TaskFn )( void *arg );

void
taskStartAsync( TaskFn *taskFn, void *arg, TaskCtrl *pctrl );

void
taskSleep( UInt32 msTimeout );  // nofail

//////////////////////////////////////////////////////////////////////////////
// Stopwatch to measure time intervals.

class Stopwatch
{
public:
                    Stopwatch( bool _start = false );

    void            start();  // nofail
    UInt64          elapsed();  // nofail

private:
    UInt64          m_startTime;
};

UInt64
timeElapsed();  // nofail, in milliseconds.

}  // namespace internal

}  // namespace webstor

#endif // !INCLUDED_SYSUTIL_H
