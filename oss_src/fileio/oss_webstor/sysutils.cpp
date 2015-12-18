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

#include "sysutils.h"

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <cross_platform/windows_wrapper.hpp>
#include <ws2tcpip.h>
#include <mstcpip.h>
#else  // !_WIN32
#include <time.h>
#include <errno.h> 
#include <sys/time.h> 
#include <sys/poll.h> 
#include <poll.h>
#include <parallel/pthread_h.h>
#include <unistd.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#endif  // !_WIN32

#include <stdarg.h>
#include <stdio.h>
#include <string.h>

#include <algorithm>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>


#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wreorder"
#pragma GCC diagnostic ignored "-Wsign-compare"

namespace webstor
{

namespace internal
{

//////////////////////////////////////////////////////////////////////////////
// Debugging support.

#ifdef DEBUG

dbgShowAssertFunc *g_dbgShowAssertFunc_ = 0;

#ifdef __GNUC__
volatile int g_fakeGlobalForDbgBreak_ = 0;
#endif

#endif  // DEBUG

//////////////////////////////////////////////////////////////////////////////
// System errors.

static void
throwSystemError( const char *op, const char *err )
{
    dbgAssert( op );

    std::string msg( op );

    if( err && *err )
    {
        msg.append( ": " );
        msg.append( err );
    }

    throw std::runtime_error( msg );
}

#ifdef _WIN32
void
throwSystemError( unsigned code, const char *op )
{
    dbgAssert( op );

    char buf[ 1024 ];

    int l = FormatMessage(
        FORMAT_MESSAGE_FROM_SYSTEM |
        FORMAT_MESSAGE_IGNORE_INSERTS,
        0,
        code,
        MAKELANGID( LANG_NEUTRAL, SUBLANG_DEFAULT ), // Default language
        buf,
        dimensionOf( buf ),
        NULL
        );

    buf[ l > 1 ? l - 2 : 0 ] = 0; // eat trailing CRLF (huh?)

    throwSystemError( op, buf );
}
#else  // !_WIN32
void
throwSystemError( unsigned code, const char *op )
{
    dbgAssert( op );

    char buf[ 1024 ];
    strerror_r( code, buf, sizeof( buf ) );
    throwSystemError( op, buf);
}
#endif  // !_WIN32


//////////////////////////////////////////////////////////////////////////////
// Stopwatch - time measurement.

Stopwatch::Stopwatch( bool _start )
{
    if( _start )
    {
        start();
    }
}

#if defined( _WIN32 ) && defined( PERF )
struct StopwatchFrequency
{
                    StopwatchFrequency();
    UInt64          value;
};

static StopwatchFrequency s_stopwatchFrequency;

StopwatchFrequency::StopwatchFrequency()
{
    LARGE_INTEGER tmp;
    QueryPerformanceFrequency( &tmp );
    value = tmp.QuadPart;
}

void 
Stopwatch::start()  // nofail
{
    LARGE_INTEGER tmp;
    QueryPerformanceCounter( &tmp );
    m_startTime = tmp.QuadPart;
}

UInt64 
Stopwatch::elapsed()  // nofail
{
    LARGE_INTEGER tmp;
    QueryPerformanceCounter( &tmp );

    return s_stopwatchFrequency.value != 0 ? 
        1000ULL * ( tmp.QuadPart - m_startTime ) / s_stopwatchFrequency.value :
        0;
}
#else  // !( defined( _WIN32 ) && defined( PERF ) )

#ifndef _WIN32
static UInt64 
GetTickCount64()
{
    timespec ts;
    struct timeval tv;
    gettimeofday(&tv, NULL);
    ts.tv_sec = tv.tv_sec;
    ts.tv_nsec = tv.tv_usec*1000;

    UInt64 result = ts.tv_sec;
    result *= 1000;  // ms, overflow is ok
    result += ts.tv_nsec / 1000000;  // ms

    return result;
}
#endif  // !_WIN32

void 
Stopwatch::start()  // nofail
{
    m_startTime = GetTickCount64();
}

UInt64 
Stopwatch::elapsed()  // nofail
{
    return GetTickCount64() - m_startTime;  // overflow is ok
}
#endif  // !( defined( _WIN32 ) && defined( PERF ) )

static Stopwatch s_stopwatch( true );

UInt64
timeElapsed()  // nofail, in milliseconds.
{
    return s_stopwatch.elapsed();
}

//////////////////////////////////////////////////////////////////////////////
// Adjustable timeout.

class Timeout
{
public:
                    Timeout( UInt32 msTimeout );
    UInt32          left();

private:
    enum { c_infinite = -1 };
    UInt32          m_timeout;
    UInt64          m_endTime;
};

Timeout::Timeout( UInt32 msTimeout )
    : m_timeout( msTimeout )
    , m_endTime( 0 )
{
    if( msTimeout != 0 && msTimeout != c_infinite )
    {
        m_endTime = GetTickCount64() + msTimeout;
    }
}

UInt32 
Timeout::left()
{
    if( m_timeout == 0 )
    {
        return 0;
    }

    if( m_timeout == c_infinite )
    {
        return c_infinite;
    }

    UInt64 leftTime = m_endTime - GetTickCount64();  // overflow ok
    return ( leftTime > m_timeout ) ? 0 : static_cast< UInt32 >( leftTime );
}

//////////////////////////////////////////////////////////////////////////////
// EventSync -- event synchronization primitive.

#ifdef _WIN32
EventSync::EventSync( bool initialState )
{
    // Create manual-reset event.

    m_handle = CreateEvent( 0, TRUE /* manualReset */, initialState ? TRUE : FALSE, NULL /* name */);

    if( !m_handle )
    {
        throwSystemError( GetLastError(), "CreateEvent" );
    }
}

EventSync::~EventSync()
{
    CloseHandle( m_handle );
}

void
EventSync::set()  // nofail
{
    dbgVerify( SetEvent( m_handle ) );
}

void
EventSync::reset()  // nofail
{
    dbgVerify( ResetEvent( m_handle ) );
}

bool
EventSync::wait( UInt32 msTimeout ) const  // nofail
{
    CASSERT( INFINITE == c_infinite );
    DWORD res = WaitForSingleObject( m_handle, msTimeout );
    dbgAssert( res == WAIT_OBJECT_0 || res == WAIT_TIMEOUT );
    return res == WAIT_OBJECT_0;
}

int     
EventSync::waitAny( EventSync **events, size_t count, UInt32 msTimeout )  
{
    CASSERT( c_maxEventCount <= MAXIMUM_WAIT_OBJECTS );
    dbgAssert( implies( count, events ) );

    if ( count > c_maxEventCount )
    {
        throw std::runtime_error( "Not supported." );
    }

    void *handles[ c_maxEventCount ] = {};
    
    for( size_t i = 0; i < count; ++i )
    {
        dbgAssert( events[ i ] );
        handles[ i ] = events[ i ]->m_handle;
    }

    DWORD res = WaitForMultipleObjects( count, handles, FALSE /* waitAll */, msTimeout );
    dbgAssert( res >= WAIT_OBJECT_0 && res < WAIT_OBJECT_0 + count || res == WAIT_TIMEOUT );

    return res == WAIT_TIMEOUT ? -1 : res - WAIT_OBJECT_0;
}

#else  // !_WIN32

EventWaiter::EventWaiter() {
  pthread_mutex_init(&lock, NULL);
  pthread_cond_init(&condvar, NULL);
}


EventSync::EventSync( bool initialState )
{
  pthread_mutex_init(&lock, NULL);
  state = 0;
}

EventSync::~EventSync()
{
  pthread_mutex_destroy(&lock);
}

void
EventSync::set()  // nofail
{
  pthread_mutex_lock(&lock);
  state = 1;
  std::set<EventWaiter*>::iterator iter = waiters_list.begin();
  while(iter != waiters_list.end()) {
    EventWaiter* waiter = *iter;
    pthread_mutex_lock(&waiter->lock);
    pthread_cond_signal(&waiter->condvar);
    pthread_mutex_unlock(&waiter->lock);
    ++iter;
  }
  pthread_mutex_unlock(&lock);
}

void
EventSync::reset()  // nofail
{
  state = 0;
}

void EventSync::add_waiter(EventWaiter* waiter) const {
  pthread_mutex_lock(&lock);
  waiters_list.insert(waiter);
  pthread_mutex_unlock(&lock);
}

void EventSync::remove_waiter(EventWaiter* waiter) const {
  pthread_mutex_lock(&lock);
  waiters_list.erase(waiter);
  pthread_mutex_unlock(&lock);
}

int EventSync::get_state() const {
  return state;
}


bool
EventSync::wait( UInt32 msTimeout ) const  // nofail
{
  EventWaiter waiter;
  add_waiter(&waiter);

  // check the state
  if (!get_state()) {
    pthread_mutex_lock(&waiter.lock);
    struct timespec timeout;
    struct timeval tv;
    gettimeofday(&tv, NULL);
    // convert ms to s and ns
    UInt32 s = msTimeout / 1000;
    msTimeout = msTimeout % 1000;
    UInt32 ns = msTimeout * 1000000;
    // convert timeval to timespec
    timeout.tv_nsec = tv.tv_usec * 1000;
    timeout.tv_sec = tv.tv_sec;

    // add the time
    timeout.tv_nsec += (suseconds_t)ns;
    timeout.tv_sec += (time_t)s;
    // shift the nsec to sec if overflow
    if (timeout.tv_nsec > 1000000000) {
      timeout.tv_sec ++;
      timeout.tv_nsec -= 1000000000;
    }
    pthread_cond_timedwait(&waiter.condvar, &waiter.lock, &timeout);
    pthread_mutex_unlock(&waiter.lock);
  }
  remove_waiter(&waiter);
  return get_state();
}

int     
EventSync::waitAny( EventSync **events, size_t count, UInt32 msTimeout )  
{
  EventWaiter waiter;
  for (size_t i = 0; i < count; ++i) {
    events[i]->add_waiter(&waiter);
  }
  // see if there are any events which are rady
  int ret = 0;
  for (size_t i = 0; i < count; ++i) {
    ret += events[i]->get_state();
  }
  if (ret) {
    for (size_t i = 0; i < count; ++i) {
      events[i]->remove_waiter(&waiter);
    }
    return ret;
  }

  pthread_mutex_lock(&waiter.lock);
  struct timespec timeout;
  struct timeval tv;
  gettimeofday(&tv, NULL);
  // convert ms to s and ns
  UInt32 s = msTimeout / 1000;
  msTimeout = msTimeout % 1000;
  UInt32 ns = msTimeout * 1000000;
  // convert timeval to timespec
  timeout.tv_nsec = tv.tv_usec * 1000;
  timeout.tv_sec = tv.tv_sec;

  // add the time
  timeout.tv_nsec += (suseconds_t)ns;
  timeout.tv_sec += (time_t)s;
  // shift the nsec to sec if overflow
  if (timeout.tv_nsec > 1000000000) {
    timeout.tv_sec ++;
    timeout.tv_nsec -= 1000000000;
  }
  pthread_cond_timedwait(&waiter.condvar, &waiter.lock, &timeout);
  pthread_mutex_unlock(&waiter.lock);
  for (size_t i = 0; i < count; ++i) {
    events[i]->remove_waiter(&waiter);
  }

  for (size_t i = 0; i < count; ++i) {
    ret += events[i]->get_state();
  }
  return ret;
}

#endif  // !_WIN32

//////////////////////////////////////////////////////////////////////////////
// ExLockSync -- exclusive lock.

#ifdef _WIN32
static inline LPCRITICAL_SECTION
pcs( void *p )
{
    return static_cast< LPCRITICAL_SECTION >( p );
}

ExLockSync::ExLockSync()
{
#ifdef DEBUG
    m_lockOwner = 0;
#endif

    CASSERT( sizeof( m_data ) == sizeof( CRITICAL_SECTION ) );
    CASSERT( __alignof( Data ) == __alignof( CRITICAL_SECTION ) );
    InitializeCriticalSection( pcs( &m_data ) );
}

ExLockSync::~ExLockSync()
{
    dbgAssert( !m_lockOwner );
    DeleteCriticalSection( pcs( &m_data ) );
}

void
ExLockSync::claimLock()  // nofail
{
    EnterCriticalSection( pcs( &m_data ) );

#ifdef DEBUG
    dbgAssert( !m_lockOwner );
    m_lockOwner = GetCurrentThreadId(); 
#endif
}

void
ExLockSync::releaseLock()  // nofail
{
#ifdef DEBUG
    dbgAssert( m_lockOwner == GetCurrentThreadId() );
    m_lockOwner = 0; 
#endif

    LeaveCriticalSection( pcs( &m_data ) );
}

#else  // !_WIN32

CASSERT( sizeof( pthread_t ) <= sizeof( UInt64 ) );

static inline pthread_mutex_t *
pmtx( void *p )
{
    return static_cast< pthread_mutex_t * >( p );
}

ExLockSync::ExLockSync()
{
#ifdef DEBUG
    m_lockOwner = 0;
#endif

    CASSERT( sizeof( m_data ) == sizeof( pthread_mutex_t ) );
    CASSERT( __alignof__( Data ) == __alignof__( pthread_mutex_t ) );

    if( int err = pthread_mutex_init( pmtx( &m_data ), 0 ) )
    {
        throwSystemError( err, "newlock" );
    }
}

ExLockSync::~ExLockSync()
{
    dbgAssert( !m_lockOwner );
    dbgVerify( !pthread_mutex_destroy( pmtx( &m_data ) ) );
}

void
ExLockSync::claimLock()  // nofail
{
    dbgVerify( !pthread_mutex_lock( pmtx( &m_data ) ) );

#ifdef DEBUG
    dbgAssert( !m_lockOwner );
    m_lockOwner = pthread_self(); 
#endif
}

void
ExLockSync::releaseLock()  // nofail
{
#ifdef DEBUG
    dbgAssert( m_lockOwner == pthread_self() );
    m_lockOwner = 0; 
#endif

    dbgVerify( !pthread_mutex_unlock( pmtx( &m_data ) ) );
}
#endif  // !_WIN32

//////////////////////////////////////////////////////////////////////////////
// SocketPool

#ifdef _WIN32

static inline 
SOCKET s( SocketHandle socket ) 
{
     CASSERT( sizeof( SOCKET ) == sizeof( SocketHandle ) );
     return reinterpret_cast< SOCKET >( socket );
}

static inline 
SocketHandle sh( SOCKET socket ) 
{
     CASSERT( sizeof( SOCKET ) == sizeof( SocketHandle ) );
     return reinterpret_cast< SocketHandle >( socket );
}

struct SocketPoolState : public std::vector< pollfd > {};

SocketPool::SocketPool() 
    : m_pool( new SocketPoolState() )
{
}

static int 
pollfdLess( const pollfd& v1, const pollfd& v2 )
{
    return v1.fd < v2.fd;
}

bool 
SocketPool::add( SocketHandle socket, SocketActionMask actionMask )  // nofail
{
    //$ WARNING: this is nofail only if SocketPool::reserve was called
    // to ensure capacity.

    UInt32 events = 
        ( actionMask & SA_POLL_IN ?  POLLIN : 0 ) |
        ( actionMask & SA_POLL_OUT ? POLLOUT : 0 );

    pollfd fd = 
    { 
        s( socket ), 
        events, 
        0 
    };

    SocketPoolState::iterator it = std::lower_bound( m_pool->begin(), m_pool->end(), fd, pollfdLess );

    if( it == m_pool->end() || it->fd != fd.fd )
    {
        // Insert a new socket.

        dbgAssert( m_pool->capacity() > m_pool->size() );
        m_pool->insert( it, fd );  // nofail because we checked capacity above.
        return true;
    }

    // Update event mask on the existing socket.

    it->events = events;
    return false;
}

bool
SocketPool::remove( SocketHandle socket )  // nofail
{
    pollfd fd = 
    { 
        s( socket ), 
        0, 
        0 
    };

    SocketPoolState::iterator it = std::lower_bound( m_pool->begin(), m_pool->end(), fd, pollfdLess );

    if( it == m_pool->end() || it->fd != fd.fd )
    {
        return false;
    }

    m_pool->erase( it );  // nofail
    return true;
}

void            
SocketPool::reserve( size_t size ) 
{ 
    m_pool->reserve( size ); 
} 

size_t          
SocketPool::size() const
{ 
    return m_pool->size(); 
}

static inline SocketActionMask
getActionMask( UInt32 events, UInt32 revents )
{
    SocketActionMask actionMask = 0;

    if( events & POLLIN ) 
    {
        if( revents & ( POLLRDNORM | POLLIN | POLLERR | POLLHUP ) )
        {
            actionMask |= SA_POLL_IN;
        }
        if( revents & ( POLLRDBAND | POLLPRI | POLLNVAL ) )
        {
            actionMask |= SA_POLL_ERR;
        }
    }

    if( events & POLLOUT ) 
    {
        if( revents & ( POLLWRNORM | POLLOUT ) )
        {
            actionMask |= SA_POLL_OUT;
        }
        if( revents & ( POLLERR | POLLHUP | POLLNVAL ) )
        {
            actionMask |= SA_POLL_ERR;
        }
    }

    return actionMask;
}

bool
SocketPool::wait( UInt32 msTimeout, UInt32 msInterruptOnlyTimeout, SocketActions *socketActions )   
{
    dbgAssert( socketActions );
    socketActions->clear();

    if( m_pool->size() == 0 )
    {
        // We don't have any sockets to check activity, so wait for 
        // the interrupt event.

#ifdef PERF
        Stopwatch stopwatch( true );
#endif

        DWORD res = WaitForSingleObject( m_interrupt.m_handle, msInterruptOnlyTimeout );
        dbgAssert( res == WAIT_OBJECT_0 || res == WAIT_TIMEOUT );

#ifdef PERF
        LOG_TRACE( "SocketPoolSync:wait for interrupt, actual=%llu, result=%d", 
            stopwatch.elapsed(), res );
#endif

        m_interrupt.reset();
        return res == WAIT_OBJECT_0;  // true if interrupt.
    }
  
    // It would be great to be able to wait for all sockets and the interrupt event together.
    // But we cannot use WSAPoll for that (because it supports sockets only) and 
    // WSAEventSelect/WaitForMultipleObjects (because they are not usable with curl for
    // write events). 
    //
    // For the latter from MSDN (WSAEventSelect):
    //   The FD_WRITE network event is handled slightly differently. An FD_WRITE network event is recorded 
    //   when a socket is first connected with a call to the connect, ConnectEx, WSAConnect, 
    //   WSAConnectByList, or WSAConnectByName function or when a socket is accepted with accept, 
    //   AcceptEx, or WSAAccept function and then after a send fails with WSAEWOULDBLOCK and 
    //   buffer space becomes available. Therefore, an application can assume that sends are 
    //   possible starting from the first FD_WRITE network event setting and lasting until 
    //   a send returns WSAEWOULDBLOCK. After such a failure the application will find out 
    //   that sends are again possible when an FD_WRITE network event is recorded and the
    //   associated event object is set.
    //
    // We don't know if the curl (or whatever caller) got WSAEWOULDBLOCK or not. And if the event is signaled,
    // we don't know if we need to reset it.
    // If curl didn't get WSAEWOULDBLOCK and we reset the event now,
    // we may not (or will never depending on how we are sending) get another signal for a while. 
    // But if we don't reset, WaitForMultipleObjects(..)
    // call becomes useless because the event is always signaled and we just get a busy loop.
    //
    // Other options such as to invoke APC to interrupt WSAPoll don't work either, APC gets
    // delivered but WSAPoll continues running and not interrupted immediately.
    //
    // As workaround, let's spin with short WSAPoll calls and check the interrupt signal in between
    // them.
    //

    dbgAssert( msTimeout < INFINITE );
    const UInt32 spinTimeout = 15;

    while( msTimeout )
    {
        // Check the interrupt signal.

        if( m_interrupt.wait( 0 ) )
        {
            m_interrupt.reset();
            return true;  // true if interrupt.
        }

#ifdef PERF
        Stopwatch stopwatch( true );
#endif
        int res = WSAPoll( &( ( *m_pool )[ 0 ] ), m_pool->size(), spinTimeout );
#ifdef PERF
        LOG_TRACE( "SocketPoolSync:WSAPoll, timeout left=%d, spin=%d, actual=%llu, size=%llu, result=%d", 
            msTimeout, spinTimeout, stopwatch.elapsed(), static_cast< UInt64 >( m_pool->size() ), res );
#endif

        dbgAssert( res >= 0 );

        if( res == 0 )
        {
            if ( msTimeout <= spinTimeout )
            {
                // Overall timeout has expired.

                break;
            }

            // Reduce the overall timeout and repeat.

            msTimeout -= spinTimeout;
            continue;
        }

        if( res > 0 )
        {
            for( SocketPoolState::const_iterator it = m_pool->begin(); it != m_pool->end(); ++it )
            {
                if( it->revents != 0 )
                {
                    socketActions->push_back( SocketActions::value_type( sh( it->fd ), 
                        getActionMask( it->events, it->revents ) ) );
                }
            }
        }

        break;
    }

    return !socketActions->empty(); // true if activity has been detected.
}

#else  // !_WIN32

static inline 
int s( SocketHandle socket ) 
{
    return socket;
}

struct SocketPoolState
{
                    SocketPoolState();
                    ~SocketPoolState();

    std::vector< SocketHandle >     sockets;
    std::vector<struct pollfd> polllist;
};


SocketPoolState::SocketPoolState()
{
}

SocketPoolState::~SocketPoolState()
{
}

SocketPool::SocketPool() 
    : m_pool( NULL )
{
    std::auto_ptr< SocketPoolState > pool( new SocketPoolState() );

    // Commit changes.

    m_pool = pool.release();  // nofail
}

bool 
SocketPool::add( SocketHandle socket, SocketActionMask actionMask )  // nofail
{
    //$ WARNING: this is nofail only if SocketPool::reserve was called
    // to ensure capacity.

    struct pollfd pfd;
    pfd.events = ( actionMask & SA_POLL_IN ? ( POLLIN | POLLHUP ) : 0 ) | 
        ( actionMask & SA_POLL_OUT ? ( POLLOUT | POLLHUP ) : 0 );
    pfd.fd = socket;

    bool found = false;
    for (size_t i = 0;i < m_pool->polllist.size(); ++i) {
      if (m_pool->polllist[i].fd == socket) {
        m_pool->polllist[i] = pfd;
        found = true;
      }
    }
    if (!found) {
      m_pool->polllist.push_back(pfd);
    }

    // Add the socket to our list.
    std::vector< SocketHandle >::iterator it = std::lower_bound( m_pool->sockets.begin(), 
        m_pool->sockets.end(), socket );  // nofail

    if( it == m_pool->sockets.end() || *it != socket )
    {
        dbgAssert( m_pool->sockets.capacity() > m_pool->sockets.size() );
        m_pool->sockets.insert( it, socket );  // nofail because we checked capacity above.
        return true;
    }

    return false;
}

bool
SocketPool::remove( SocketHandle socket )  // nofail
{
    for (size_t i = 0; i < m_pool->polllist.size(); ++i) {
      if (m_pool->polllist[i].fd == socket) {
        m_pool->polllist.erase(m_pool->polllist.begin() + i);
        break;
      }
    }

    std::vector< SocketHandle >::iterator it = std::lower_bound( m_pool->sockets.begin(), m_pool->sockets.end(), socket );

    if( it == m_pool->sockets.end() || *it != socket )
    {
        return false;
    }

    m_pool->sockets.erase( it );  // nofail
    return true;
}

void
SocketPool::reserve( size_t size ) 
{
    m_pool->sockets.reserve( size );
}

size_t
SocketPool::size() const
{ 
    return m_pool->sockets.size();
}

static inline SocketActionMask
getActionMask( UInt32 events )
{
    SocketActionMask actionMask = 0;

    if( events & ( POLLIN  | POLLERR | POLLHUP ) )
    {
        actionMask |= SA_POLL_IN;
    }

    if( events & ( POLLOUT ) )
    {
        actionMask |= SA_POLL_OUT;
    }

    if( events & ( POLLERR | POLLHUP  | POLLPRI ) )
    {
        actionMask |= SA_POLL_ERR;
    }

    return actionMask;
}

bool
SocketPool::wait( UInt32 msTimeout, UInt32 msInterruptOnlyTimeout, SocketActions *socketActions )
{
    dbgAssert( socketActions );
    socketActions->clear();

    int res = 0;

    // If we have at least one socket, make sure that timeout is not infinite,
    // see the comments in the add(..) method.
    dbgAssert( !m_pool->sockets.size() || msTimeout < ( UInt32 ) -1 );

    UInt32 initTimeout = m_pool->sockets.size() > 0 ? msTimeout : msInterruptOnlyTimeout;
    Timeout timeout( initTimeout ); 

    bool interrupt_triggered = false;
    while( true ) 
    {
#ifdef PERF
        Stopwatch stopwatch( true );
#endif
        for (size_t i = 0;i < m_pool->polllist.size(); ++i) {
          m_pool->polllist[i].revents = 0;
        }
        // 15ms timeout
        res = poll( m_pool->polllist.data(), m_pool->polllist.size(), 15);

#ifdef PERF
        LOG_TRACE( "SocketPoolSync:poll_wait, timeout=%d, actual=%llu, size=%llu, result=%d", 
            initTimeout, stopwatch.elapsed(), static_cast< UInt64 >( m_pool->sockets.size() ), res );
#endif
        if (m_interrupt.get_state()) interrupt_triggered = true;
        if( res == -1 )
        {
            int e = errno;

            if( e == EINTR )
            {
                // Make sure we don't end up in infinite loop when timeout is not infinite.

                if( timeout.left() )
                    continue;

                break;
            }

            if( e == ENOMEM )
            {
                // Out of memory, we cannot do anything better than just wait for a while.

                taskSleep( 3000 );
                break;
            }

            dbgPanicSz( "BUG: poll_wait failed!!!" );
        }

        break;
    } 

    // Get the events.

    if (interrupt_triggered) m_interrupt.reset();
    size_t eventCount = 0;
    for (size_t i = 0;i < m_pool->polllist.size(); ++i) {
      if (m_pool->polllist[i].revents != 0) {
        socketActions->push_back( SocketActions::value_type( m_pool->polllist[i].fd,
                                                          getActionMask( m_pool->polllist[i].revents ) ) );
        ++eventCount;
      }
    }

    return eventCount != 0;  // true if socket activity or interrupt.
}
#endif  // !_WIN32

SocketPool::~SocketPool()
{
    delete m_pool;
}

void 
SocketPool::signal()  // nofail
{ 
    m_interrupt.set(); 
} 


//////////////////////////////////////////////////////////////////////////////
// Socket tuning.

#ifdef _WIN32

void
setTcpKeepAlive( SocketHandle socket, const TcpKeepAliveParams *params )
{
    // Set TCP KeepAlive.

    // probeCount can be set only through registry:
    // HKLM\SYSTEM\CurrentControlSet\Services\Tcpip\Parameters [DWORD TcpMaxDataRetransmissions]
    // On Windows Vista and later, the number of keep-alive probes (data retransmissions) 
    // is set to 10.
    // On Windows Server 2003, Windows XP, and Windows 2000, 
    // the default setting for number of keep-alive probes is 5.

    tcp_keepalive vals = {};
    vals.keepalivetime = params ? params->probeStartTime : 0;        //  milliseconds  
    vals.keepaliveinterval = params ? params->probeIntervalTime : 0; //  milliseconds  
    vals.onoff = params ? 1 : 0; 
    DWORD unused = 0; 

    int res = WSAIoctl( s( socket ), SIO_KEEPALIVE_VALS, 
        &vals, sizeof( vals ), NULL, 0, &unused, NULL, NULL );
    dbgAssert( !res );
}

#else  // !_WIN32

void
setTcpKeepAlive( SocketHandle socket, const TcpKeepAliveParams *params )
{
    if( params )
    {
        int val = params->probeStartTime / 1000;  // secs
#ifdef __APPLE__
        dbgVerify( !setsockopt( socket, IPPROTO_TCP, TCP_KEEPALIVE, &val, sizeof( val ) ) );
#else
        dbgVerify( !setsockopt( socket, IPPROTO_TCP, TCP_KEEPIDLE, &val, sizeof( val ) ) );
#endif

        val = params->probeIntervalTime / 1000; // secs
        dbgVerify( !setsockopt( socket, IPPROTO_TCP, TCP_KEEPINTVL, &val, sizeof( val ) ) );

        dbgVerify( !setsockopt( socket, IPPROTO_TCP, TCP_KEEPCNT, &params->probeCount, 
            sizeof( params->probeCount ) ) );
    }

    int val = params ? 1 : 0; 
    int res = setsockopt( socket, SOL_SOCKET, SO_KEEPALIVE, &val, sizeof( val ) );
    dbgAssert( !res );  
}

#endif  // !_WIN32

void
setSocketBuffers( SocketHandle socket, UInt32 size )
{
    setsockopt( s( socket ), SOL_SOCKET, SO_SNDBUF, 
        reinterpret_cast< const char * >( &size ), sizeof( size ) );
    
    setsockopt( s( socket ), SOL_SOCKET, SO_RCVBUF, 
        reinterpret_cast< const char * >( &size ), sizeof( size ) );
}

//////////////////////////////////////////////////////////////////////////////
// TaskCtrl -- asynchronous task control and Task utilities.

#ifdef _WIN32
void
TaskCtrlDeleter::free( TaskHandle handle )
{
    dbgVerify( CloseHandle( handle ) );
}

void
TaskCtrl::wait() const  // nofail
{
    dbgAssert( !empty() );
    dbgVerify( WaitForSingleObject( *this, INFINITE ) == WAIT_OBJECT_0 );
}

void
taskStartAsync( TaskFn *taskFn, void *arg, TaskCtrl *pctrl )
{
    dbgAssert( taskFn );
    dbgAssert( pctrl );

    DWORD id;
    HANDLE const h = CreateThread( 0, 0, taskFn, arg, 0, &id );

    if( !h )
        throwSystemError( GetLastError(), "starttask" );

    pctrl->reset( h );
}

void
taskSleep( UInt32 msTimeout )  // nofail
{
    Sleep( msTimeout );
}

#else  // !_WIN32

void
TaskCtrlDeleter::free( TaskHandle handle )
{
    dbgAssert( handle );
    dbgVerify( pthread_detach( handle ) );
}

void
TaskCtrl::wait() const  // nofail
{
    if( empty() )
        return;

    // The docs say that a thread can be joined or detached exactly once,
    // so we empty the handle.

    dbgVerify( !pthread_join( const_cast< TaskCtrl * >( this )->release(), 0 ) );
}


CASSERT( sizeof( pthread_t ) <= sizeof( TaskId ) );

void
taskStartAsync( TaskFn *taskFn, void *arg, TaskCtrl *pctrl )
{
    dbgAssert( taskFn );
    dbgAssert( pctrl );

    pthread_t thd = 0;

    if( int err = pthread_create( &thd, 0, taskFn, arg ) )
        throwSystemError( err, "starttask" );

    dbgAssert( thd );
    pctrl->reset( thd );
}

void
taskSleep( UInt32 msTimeout )  // nofail
{
    timespec ts = { msTimeout / 1000, msTimeout % 1000 * 1000000 };

    while( nanosleep( &ts, &ts ) == -1 )
    {
        if( errno != EINTR )
        {
            dbgPanicSz( "BUG: nanosleep failed!!!" );
            break;
        }

        // in case of EINTR the timeout is supposed to be adjusted to repeat.
    }
}
#endif  // !_WIN32

#ifdef WEBSTOR_ENABLE_DBG_TRACING
//////////////////////////////////////////////////////////////////////////////
// Tracing support.

static FILE *s_traceFile = fopen( "trace.log", "ab" );
static ExLockSync s_traceFilelock;

void 
logTrace( const char *fmt, ... )
{
    // Silently ignore errors, but don't crash if
    // couldn't create trace file.

    if( s_traceFile )
    {

        va_list args;
        va_start( args, fmt );

        s_traceFilelock.claimLock();  // nofail
        vfprintf( s_traceFile, fmt, args );
        s_traceFilelock.releaseLock();

        va_end( args );
    }
}
#endif  // WEBSTOR_ENABLE_DBG_TRACING

}  // namespace internal

}  // namespace webstor
