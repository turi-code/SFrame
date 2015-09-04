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
// Author: Maxim Mazeev <mazeev@hotmail.com>

//////////////////////////////////////////////////////////////////////////////
// Standalone WebStor command line app.
//////////////////////////////////////////////////////////////////////////////

#define WS_CMD_CLI_COMPAT

#include "sysutils.h"
#include "wsconn.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#ifdef _WIN32 
#define snprintf sprintf_s 
#endif 

#pragma GCC diagnostic ignored "-Wreorder"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wunused-function"

using namespace webstor;
using namespace webstor::internal;

static char s_errMsg[ 512 ] = {};
static const UInt64 s_verboseInterval = 3000; // in msecs.
static const size_t MB = 1024 * 1024;

// Note: this option list must be consistent with usage() and parseCommandLine(..)
// methods.

static StringWithLen s_cmdFlags = 
#ifdef WS_CMD_CLI_COMPAT
    { STRING_WITH_LEN( "-i -s -H -U -P -G -a -f -n -p -m -x -d -z -b -v -help --help -? --? -W -k" ) };
#else
    { STRING_WITH_LEN( "-i -s -H -U -P -G -a -f -n -p -m -x -d -z -b -v -help --help -? --?" ) };
#endif

static void
usage()
{
    std::cout << 
        "wscmd options:                                                                 \n"
        "                                                                               \n"
        "    -i mandatory cloud storage access key,                                     \n"
        "       (it can be specified via WS_ACCESS_KEY env. variable)                   \n"
        "    -s mandatory cloud storage secret key,                                     \n"
        "       (it can be specified via WS_SECRET_KEY env. variable)                   \n"
        "    -H optional region-specific endpoint or a mandatory Walrus host name,      \n"
        "       (it can be specified via WS_HOST env. variable)                         \n"
        "    -P optional port number,                                                   \n"
        "    -U (optional flag to use HTTP instead of HTTPS),                           \n"
        "    -G optional proxy with port number (proxy:port),                           \n"
        "       (it can be specified via WS_PROXY env. variable)                        \n"
        "    -a action, one of the following:                                           \n"
        "       createBucket                                                            \n"
        "       delBucket                                                               \n"
        "       listAllBuckets                                                          \n"
        "       put                                                                     \n"
        "       get                                                                     \n"
        "       del                                                                     \n"
        "       delAll (delete by prefix)                                               \n"
        "       listAllObjects                                                          \n"
        "       listAllMultipartUploads                                                 \n"
        "       abortAllMultipartUploads                                                \n"
        "                                                                               \n"
        "    action-specific parameters, some of them mandatory depending on action:    \n"
        "                                                                               \n"
        "    -f filename (for 'put' and 'get'),                                         \n"
        "    -n bucket name (all except for 'listAllBuckets'),                          \n"
        "       (it can be specified via WS_BUCKET_NAME env. variable)                  \n"
        "    -p key or key prefix (all except for bucket-related actions),              \n"
        "    -m marker for entries to list (for 'listAllObjects' and                    \n"
        "       'listAllMultipartUploads'),                                             \n"
        "    -x maximum number of entries to list (for 'listAllObjects' and             \n"
        "       'listAllMultipartUploads'),                                             \n"
        "    -d delimiter to list directories (for 'listAllObjects' and                 \n"
        "       'listAllMultipartUploads'),                                             \n"
        "    -z chunk size in MB (for 'put' to enable multipart upload,                 \n"
        "       size must be 5MB minimum, not supported by Walrus),                     \n"
        "    -b make public (for 'createBucket' and 'put'),                             \n"
        "    -v verbose mode.                                                           \n"
        "                                                                               \n"
        "Some of options can be specified through env. variables:                       \n"
        "    WS_ACCESS_KEY  - instead of option '-i',                                   \n"
        "    WS_SECRET_KEY  - instead of option '-s',                                   \n"
        "    WS_HOST        - instead of option '-H',                                   \n"
        "    WS_BUCKET_NAME - instead of option '-n',                                   \n"
        "    WS_PROXY       - instead of option '-G',                                   \n"
        "                                                                               \n"
        "Notes:                                                                         \n"
        "    If you specify '-z' flag and upload doesn't finish because of crash or     \n"
        "    connection issues, orphan chunks can be left in Amazon S3 storage.         \n"
        "    It's recommended to execute 'listAllMultipartUploads' and                  \n"
        "    'abortAllMultipartUploads' actions to purge them.                          \n"
        "                                                                               \n"
        "Examples:                                                                      \n"
        "                                                                               \n"
        " * create a new bucket:                                                        \n"
        "   wscmd -i WS_ACCESS_KEY -s WS_SECRET_KEY -a createBucket -n mybucket         \n"
        "                                                                               \n"
        " * delete a bucket:                                                            \n"
        "   wscmd -i WS_ACCESS_KEY -s WS_SECRET_KEY -a delBucket -n mybucket            \n"
        "                                                                               \n"
        " * list all buckets:                                                           \n"
        "   wscmd -i WS_ACCESS_KEY -s WS_SECRET_KEY -a listAllBuckets                   \n"
        "                                                                               \n"
        " * upload a file:                                                              \n"
        "   wscmd -i WS_ACCESS_KEY -s WS_SECRET_KEY -a put -n mybucket                  \n"
        "   -f image.jpg -p folder/image.jpg -v                                         \n"
        "                                                                               \n"
        " * upload a large file using multipart upload:                                 \n"
        "   wscmd -i WS_ACCESS_KEY -s WS_SECRET_KEY -a put -n mybucket                  \n"
        "   -f image.jpg -p folder/image.jpg -z 5 -v                                    \n"
        "                                                                               \n"
        " * download a file:                                                            \n"
        "   wscmd -i WS_ACCESS_KEY -s WS_SECRET_KEY -a get -n mybucket                  \n"
        "   -f image.jpg -p folder/image.jpg -v                                         \n"
        "                                                                               \n"
        " * delete an object:                                                           \n"
        "   wscmd -i WS_ACCESS_KEY -s WS_SECRET_KEY -a del -n mybucket                  \n"
        "   -p folder/image.jpg                                                         \n"
        "                                                                               \n"
        " * delete all objects for a given prefix (e.g. from a directory):              \n"
        "   wscmd -i WS_ACCESS_KEY -s WS_SECRET_KEY -a delAll -n mybucket -p folder/    \n"
        "                                                                               \n"
        " * list all objects:                                                           \n"
        "   wscmd -i WS_ACCESS_KEY -s WS_SECRET_KEY -a listAllObjects -n mybucket       \n"
        "                                                                               \n"
        " * list all objects with a given prefix (e.g. all objects in a directory):     \n"
        "   wscmd -i WS_ACCESS_KEY -s WS_SECRET_KEY -a listAllObjects -n mybucket       \n"
        "   -p folder/                                                                  \n"
        "                                                                               \n"
        " * list all top-level directories:                                             \n"
        "   wscmd -i WS_ACCESS_KEY -s WS_SECRET_KEY -a listAllObjects -n mybucket -d /  \n"
        "                                                                               \n"
        " * list all sub directories:                                                   \n"
        "   wscmd -i WS_ACCESS_KEY -s WS_SECRET_KEY -a listAllObjects -n mybucket       \n"
        "   -p folder/ -d /                                                             \n";
}


struct Options
{
    Options()
        : isHttps( true )
        , chunkSize( 0 )
        , makePublic( false )
        , showUsage( false ) 
        , verbose( false )
        , maxKeys( 0 )
    {}

    std::string accKey;
    std::string secKey;
    std::string host;
    std::string port;
    bool isHttps;
    std::string proxy;
    std::string action;
    std::string filename;
    std::string bucketName;
    std::string prefix;
    std::string marker;
    size_t maxKeys;
    std::string delimiter;
    size_t chunkSize;
    bool makePublic;
    bool showUsage;
    bool verbose;
};

struct Statistics
{
    Statistics()
        : dataTransfered( 0 )
    {}

    size_t dataTransfered;
};

//////////////////////////////////////////////////////////////////////////////
// Cmdline parsing.

static bool 
isCmdFlag( const char *value );

static bool
tryGetValue( const char *flag, int *i, int argc, char **argv, std::string *field )
{
    dbgAssert( *i >= 0 && *i < argc );
    dbgAssert( isCmdFlag( flag ) );

    if( !strcmp( argv[ *i ], flag ) )
    {
        const char *value = NULL;

        if( *i + 1 < argc && !isCmdFlag( argv[ *i + 1 ] ) )
        {
            *field = argv[ *i + 1 ];
            ++( *i );
            return true;
        }
        else
        {
            snprintf( s_errMsg, sizeof( s_errMsg ) - 1, "Value is missing for %s.", flag );
            throw s_errMsg;
        }
    }

    return false;
}

static bool
tryGetValue( const char *flag, int *i, int argc, char **argv, size_t *field )
{
    dbgAssert( *i >= 0 && *i < argc );
    dbgAssert( isCmdFlag( flag ) );

    if( !strcmp( argv[ *i ], flag ) )
    {
        if( *i + 1 < argc && !isCmdFlag( argv[ *i + 1 ] ) )
        {
            *field = atoi( argv[ *i + 1 ] );
            ++( *i );
            return true;
        }
        else
        {
            snprintf( s_errMsg, sizeof( s_errMsg ) - 1, "Value is missing for %s.", flag );
            throw s_errMsg;
        }
    }

    return false;
}

static bool
tryGetValue( const char *flag, int *i, int argc, char **argv, bool *field, bool value = true )
{
    dbgAssert( *i >= 0 && *i < argc );
    dbgAssert( isCmdFlag( flag ) );

    if( !strcmp( argv[ *i ], flag ) )
    {
        *field = value;
        return true;
    }

    return false;
}

static void
readEnvVar( const char *var, std::string *field )
{
    dbgAssert( var );
    dbgAssert( field );

    const char *value = getenv( var );

    if( value && *value )
    {
        field->assign( value );
    }
}

static void
readEnvVars( Options *options )
{
    dbgAssert( options );

    readEnvVar( "WS_ACCESS_KEY", &options->accKey );
    readEnvVar( "WS_SECRET_KEY", &options->secKey );
    readEnvVar( "WS_BUCKET_NAME", &options->bucketName );
    readEnvVar( "WS_HOST", &options->host );
    readEnvVar( "WS_PROXY", &options->proxy );
}


static bool 
isCmdFlag( const char *value )
{
    dbgAssert( value && *value );
    const char *p = strstr( s_cmdFlags.str, value );

    if( !p )
    {
        return false;
    }

    size_t len = strlen( value );
    return ( p == s_cmdFlags.str || *( p - 1 ) == ' ' ) &&
        ( p + len >= s_cmdFlags.str || *( p + len ) == ' ' );
}

static void
parseCommandLine( int argc, char **argv, Options *options )
{
    dbgAssert( options );

#ifdef WS_CMD_CLI_COMPAT
    bool unused = false;
#endif

    for( int i = 1; i < argc; i++ )
    {
        // Note: option flags are taken from s_cmdFlags.
        // If you add a new, don't forget to update s_cmdFlags.

        if( tryGetValue( "-i", &i, argc, argv, &options->accKey ) ||
            tryGetValue( "-s", &i, argc, argv, &options->secKey ) ||
            tryGetValue( "-H", &i, argc, argv, &options->host ) ||
            tryGetValue( "-U", &i, argc, argv, &options->isHttps, false ) ||
            tryGetValue( "-P", &i, argc, argv, &options->port ) ||
            tryGetValue( "-G", &i, argc, argv, &options->proxy ) ||
            tryGetValue( "-a", &i, argc, argv, &options->action ) ||
            tryGetValue( "-f", &i, argc, argv, &options->filename ) ||
            tryGetValue( "-n", &i, argc, argv, &options->bucketName ) ||
            tryGetValue( "-p", &i, argc, argv, &options->prefix ) ||
            tryGetValue( "-m", &i, argc, argv, &options->marker ) ||
            tryGetValue( "-x", &i, argc, argv, &options->maxKeys ) ||
            tryGetValue( "-d", &i, argc, argv, &options->delimiter ) ||
            tryGetValue( "-z", &i, argc, argv, &options->chunkSize ) ||
            tryGetValue( "-b", &i, argc, argv, &options->makePublic ) ||
            tryGetValue( "-v", &i, argc, argv, &options->verbose ) ||
            tryGetValue( "--help", &i, argc, argv, &options->showUsage ) ||
            tryGetValue( "-help", &i, argc, argv, &options->showUsage ) ||
            tryGetValue( "-?", &i, argc, argv, &options->showUsage ) ||
            tryGetValue( "--?", &i, argc, argv, &options->showUsage ) 
#ifdef WS_CMD_CLI_COMPAT
            || tryGetValue( "-k", &i, argc, argv, &options->prefix ) 
            || tryGetValue( "-W", &i, argc, argv, &unused )
#endif
            
            )
        {
            continue;
        }

        snprintf( s_errMsg, sizeof( s_errMsg ) - 1, "Invalid option '%s'.", argv[ i ] );
        throw s_errMsg;
    }
}


//////////////////////////////////////////////////////////////////////////////
// Verification of option fields.

static void
checkSpecified( const std::string &value, const char *errMsg )
{
    if( value.empty() )
    {
        throw errMsg;
    }
}

static void
checkAccessKey( const Options &options )
{
    checkSpecified( options.accKey, "Cloud storage access key is not specified. You need to provide '-i accessKey' option." );
}

static void
checkSecretKey( const Options &options )
{
    checkSpecified( options.secKey, "Cloud storage secret key is not specified. You need to provide '-s secretKey' option." );
}

static void
checkAction( const Options &options )
{
    checkSpecified( options.action, "Action is not specified. You need to provide '-a action' option." );
}

static void
checkFileName( const Options &options )
{
    checkSpecified( options.filename, "file name is not specified. You need to provide '-f filename' option." );
}

static void
checkBucketName( const Options &options )
{
    checkSpecified( options.bucketName, "bucket name is not specified. You need to provide '-n bucketName' option." );
}

static void
checkKey( const Options &options )
{
    checkSpecified( options.prefix, "key is not specified. You need to provide '-p key' option." );
}

static void
checkChunkSize( const Options &options )
{
    if( options.chunkSize < WsConnection::c_multipartUploadMinPartSizeMB )
    {
        snprintf( s_errMsg, sizeof( s_errMsg ) - 1, 
            "Invalid chunkSize '%llu'. Check '-z chunkSize' option, it must be 5MB minimum (chunkSize value is MB).", 
            static_cast< UInt64 >( options.chunkSize ) );
        throw s_errMsg;
    }

    if( options.chunkSize >= 1000000 )
    {
        snprintf( s_errMsg, sizeof( s_errMsg ) - 1, 
            "Too large chunkSize '%llu'. Check '-z chunkSize' option, it's in MB.", 
            static_cast< UInt64 >( options.chunkSize ) );
        throw s_errMsg;
    }
}


//////////////////////////////////////////////////////////////////////////////
// 'ListXXX' actions.

static void
listAllBuckets( WsConnection *conn, const Options &options )
{
    dbgAssert( conn );

    std::vector< WsBucket > buckets;
    size_t index = 0;

    conn->listAllBuckets( &buckets );

    for( std::vector< WsBucket >::const_iterator it = buckets.begin(); it != buckets.end(); ++it )
    {
        std::cout << '[' << index++ << "] " << 
            it->name << ' ' << 
            it->creationDate << std::endl;
    }

    if(! index )
    {
        std::cout << "<empty>" << std::endl;
    }
}

static void
listAllObjects( WsConnection *conn, const Options &options )
{
    dbgAssert( conn );

    checkBucketName( options );

    std::vector< WsObject > objects;
    WsListObjectsResponse response;
    size_t index = 0;

    response.nextMarker = options.marker;

    do
    {
        objects.clear();
        conn->listObjects( options.bucketName.c_str(), options.prefix.c_str(), 
            response.nextMarker.c_str(), options.delimiter.c_str(), 
            ( options.maxKeys == 0 ? 1000 : options.maxKeys ), &objects, &response );

        for( std::vector< WsObject >::const_iterator it = objects.begin(); it != objects.end(); ++it )
        {
            std::cout << '[' << index++ << "] " << 
                ( it->isDir ? "D " : " " ) <<
                it->key << ' ' << 
                it->size << ' ' <<
                it->lastModified << std::endl;
        }
    }
    while( response.isTruncated && options.maxKeys == 0 );

    if(! index )
    {
         std::cout << "<empty>" << std::endl;
    }
}

static void
listAllMultipartUploads( WsConnection *conn, const Options &options )
{
    dbgAssert( conn );

    checkBucketName( options );

    std::vector< WsMultipartUpload > uploads;
    WsListMultipartUploadsResponse response;
    size_t index = 0;

    response.nextKeyMarker = options.marker;

    do
    {
        uploads.clear();
        conn->listMultipartUploads( options.bucketName.c_str(), options.prefix.c_str(), 
            response.nextKeyMarker.c_str(), 
            response.nextUploadIdMarker.c_str(), 
            options.delimiter.c_str(), 
            ( options.maxKeys == 0 ? 1000 : options.maxKeys ), 
            &uploads, &response );

        for( std::vector< WsMultipartUpload >::const_iterator it = uploads.begin(); it != uploads.end(); ++it )
        {
            std::cout << '[' << index++ << "] " << 
                ( it->isDir ? "D " : " " ) <<
                it->key << ' ' << 
                it->uploadId << std::endl;
        }
    }
    while( response.isTruncated && options.maxKeys == 0 );

    if( !index )
    {
        std::cout << "<empty>" << std::endl;
    }
}


//////////////////////////////////////////////////////////////////////////////
// 'Put' action.

class StreamUploader : public WsPutRequestUploader
{
public:
                    StreamUploader( std::istream *stream, bool verbose = false );
    size_t          totalSize() const { return m_totalSize; }
    virtual size_t  onUpload( void *chunkBuf, size_t chunkSize );
    
private:
    bool            fill();

    std::istream   *m_stream;
    std::vector< char > m_buf;
    char           *m_p;
    size_t          m_size;

    size_t          m_totalSize;
    size_t          m_totalSent;

    bool            m_verbose;
    Stopwatch       m_stopwatch;
};

StreamUploader::StreamUploader( std::istream *stream, bool verbose )
    : m_stream( stream )
    , m_size( 0 )
    , m_totalSize( 0 )
    , m_totalSent( 0 )
    , m_p( NULL )
    , m_verbose( verbose )
    , m_stopwatch( true )
{
    dbgAssert( stream );
    dbgAssert( *stream );

    // Get file size.

    stream->seekg( 0, std::ios::end );
    m_totalSize = stream->tellg();
    stream->seekg( 0, std::ios::beg );

    // Resize buffer and fill it.

    m_buf.resize( std::min( m_totalSize + !m_totalSize, ( size_t )64 * 1024 ) );
    fill();
}

bool            
StreamUploader::fill()
{
    dbgAssert( !m_size );

    if( m_stream->eof() )
    {
        // Nothing to read.

        return false;
    }

    // Read.

    m_stream->read( &m_buf[ 0 ], m_buf.size() );

    // Reset position.

    m_p = &m_buf[ 0 ];
    m_size = m_stream->gcount();

    return true;
}

size_t
StreamUploader::onUpload( void *chunkBuf, size_t chunkSize )
{
    size_t copied = 0;

    while( true )
    {
        if( !m_size && !fill() )
        {
            // End of file reached.

            break;
        }

        // Copy min.

        size_t toCopy = std::min( m_size, chunkSize );
        memcpy( chunkBuf, m_p, toCopy );

        chunkSize -= toCopy;
        m_size -= toCopy;
        m_p += toCopy;
        copied += toCopy;

        if( !chunkSize )
        {
            // The provided buffer is full.

            break;
        }

        // Need to read more from the file, there is some space left in the provided 
        // buffer.

        chunkBuf = reinterpret_cast< char *>( chunkBuf ) + toCopy;
    }


    // Print progress.

    m_totalSent += copied;

    if( m_verbose && m_stopwatch.elapsed() > s_verboseInterval )
    {
        std::cout << "Sent: " << m_totalSent << std::endl;
        m_stopwatch.start();
    }

    return copied;
}

static void 
put( WsConnection *conn, StreamUploader *uploader, const Options &options )
{
    dbgAssert( conn );
    dbgAssert( uploader );

    // Execute a single put.

    WsPutResponse response;
    conn->put( options.bucketName.c_str(), options.prefix.c_str(), 
        uploader, uploader->totalSize(),
        NULL /* contentType */, conn->c_noCacheControl,
        options.makePublic, false /* useSrvEncrypt */,
        &response );

    std::cout << "Uploaded: " << uploader->totalSize() << std::endl;
}

static void 
multiput( WsConnection *conn, StreamUploader *uploader, const Options &options )
{
    dbgAssert( conn );
    dbgAssert( uploader );

    // Execute multipart upload.

    size_t totalSize = uploader->totalSize();
    size_t left = totalSize;
    size_t chunkSize = options.chunkSize * MB;

    // Initiate multipart upload.

    WsInitiateMultipartUploadResponse initResponse;
    conn->initiateMultipartUpload( options.bucketName.c_str(), options.prefix.c_str(),
        NULL /* contentType */, conn->c_noCacheControl,
        options.makePublic, false /* useSrvEncrypt */,
        &initResponse );

    std::vector< WsPutResponse > parts;

    for( int partNumber = 1; left; ++partNumber )
    {
        // Upload a chunk.

        size_t toPut = std::min( left, chunkSize );

        WsPutResponse putResponse;
        conn->putPart( options.bucketName.c_str(), options.prefix.c_str(), initResponse.uploadId.c_str(), 
            partNumber, uploader, toPut, &putResponse );

        left -= toPut;

        // Remember the chunk etag and number.

        parts.push_back( putResponse );

        // Print progress.

        std::cout << "Uploaded: " << totalSize - left;
        
        if( left && partNumber == 1 )
        {
            std::cout << " of " << totalSize;
        }
        std::cout << std::endl;
    }

    // Commit the upload.

    WsCompleteMultipartUploadResponse completeResponse;
    conn->completeMultipartUpload( options.bucketName.c_str(), options.prefix.c_str(),
        initResponse.uploadId.c_str(), &parts[ 0 ], parts.size(), &completeResponse );
}

static void
put( WsConnection *conn, const Options &options, Statistics *stat )
{
    dbgAssert( conn );
    dbgAssert( stat );

    checkBucketName( options );
    checkKey( options );
    checkFileName( options );

    // Open the file for reading.

    std::fstream stream;
    stream.open( options.filename.c_str(), std::ifstream::in | std::ifstream::binary );

    if( !stream )
    {
        snprintf( s_errMsg, sizeof( s_errMsg ) - 1, "Cannot open file '%s' for reading.", 
            options.filename.c_str() );
        throw s_errMsg;
    }

    // Prepare uploader.

    StreamUploader uploader( &stream, options.verbose );

    if( options.chunkSize )
    {
        // Execute multipart upload.

        checkChunkSize( options );
        multiput( conn, &uploader, options );
    }
    else
    {
        // Execute single put.

        put( conn, &uploader, options );
    }

    stat->dataTransfered = uploader.totalSize();
}


//////////////////////////////////////////////////////////////////////////////
// 'Get' action.

class StreamLoader : public WsGetResponseLoader
{
public:
                    StreamLoader( std::ostream *stream, bool verbose = false );
                    ~StreamLoader();

    void            flush();  // nofail

    virtual size_t  onLoad( const void *chunkData, size_t chunkSize, size_t totalSizeHint ); 
    
private:

    std::ostream   *m_stream;
    std::vector< char > m_buf;
    char           *m_p;
    size_t          m_size;
    size_t          m_totalReceived;

    bool            m_verbose;
    Stopwatch       m_stopwatch;
};

StreamLoader::StreamLoader( std::ostream *stream, bool verbose )
    : m_stream( stream )
    , m_p( NULL )
    , m_size( 0 )
    , m_totalReceived( 0 )
    , m_verbose( verbose )
    , m_stopwatch( true )
{
    dbgAssert( stream );
    dbgAssert( *stream );
}

StreamLoader::~StreamLoader()
{
    flush(); // nofail
}

void            
StreamLoader::flush() // nofail
{
    if( !m_size )
    {
        // Nothing to flush.

        return;
    }

    // Write the buffer.

    m_stream->write( &m_buf[ 0 ], m_size );

    // Reset the position.

    m_p = &m_buf[ 0 ];
    m_size = 0;
}

size_t
StreamLoader::onLoad( const void *chunkData, size_t chunkSize, size_t totalSizeHint )
{
    if( m_buf.size() == 0 )
    {
        // Allocate a buffer.

        size_t bufferSize = 64 * 1024;

        if( totalSizeHint != 0 && totalSizeHint < bufferSize )
        {
            bufferSize = totalSizeHint;
        }

        m_buf.resize( bufferSize );
        m_p = &m_buf[ 0 ];
    }

    size_t copied = 0;

    while( true )
    {
        // Copy min.

        size_t toCopy = std::min( m_buf.size() - m_size, chunkSize );
        memcpy( m_p, chunkData, toCopy );

        chunkSize -= toCopy;
        m_size += toCopy;
        m_p += toCopy;
        copied += toCopy;

        if( !chunkSize )
        {
            // Copied everything from the provided buffer.

            break;
        }

        // Need to flush to the file, there is more to read from the provided 
        // buffer.

        chunkData = reinterpret_cast< const char *>( chunkData ) + toCopy;
        flush();
    }

    // Print progress.

    m_totalReceived += copied;

    if( m_verbose && m_stopwatch.elapsed() > s_verboseInterval )
    {
        std::cout << "Received: " << m_totalReceived << std::endl;
        m_stopwatch.start();
    }

    return copied;
}

static void
get( WsConnection *conn, const Options &options, Statistics *stat )
{
    dbgAssert( conn );
    dbgAssert( stat );

    checkBucketName( options );
    checkKey( options );
    checkFileName( options );

    // Open the file for write.

    std::fstream stream;
    stream.open( options.filename.c_str(), 
        std::ofstream::trunc | std::ofstream::out | std::ifstream::binary );

    if( !stream )
    {
        snprintf( s_errMsg, sizeof( s_errMsg ) - 1, "Cannot open file '%s' for writing.", 
            options.filename.c_str() );
        throw s_errMsg;
    }

    // Prepare loader.

    StreamLoader loader( &stream, options.verbose );

    // Execute a single get.

    WsGetResponse response;
    conn->get( options.bucketName.c_str(), options.prefix.c_str(), &loader, &response );
    
    std::cout << "Downloaded: " << response.loadedContentLength << std::endl;

    stat->dataTransfered = response.loadedContentLength;
}

//////////////////////////////////////////////////////////////////////////////
// Execute actions.

static void
execute( const Options &options, Statistics *stat )
{
    checkAccessKey( options );
    checkSecretKey( options );

    // Infer storage type from the host name.

    WsStorType storType = WST_S3;

    if( !options.host.empty() )
    {
        if( strstr( options.host.c_str(), ".amazonaws.com" ) )
            storType = WST_S3;
        else if( strstr( options.host.c_str(), ".googleapis.com" ) )
            storType = WST_GCS;
        else
            storType = WST_WALRUS;
    }

    // Open connection.

    WsConfig config = {};
    config.accKey = options.accKey.c_str();
    config.secKey = options.secKey.c_str();
    config.host = options.host.c_str();
    config.storType = storType;
    config.isHttps = storType != WST_WALRUS && options.isHttps;
    config.port = options.port.c_str();
    config.proxy = options.proxy.c_str();

    WsConnection conn( config );

    // Execute a single action.

    if( !strcmp( options.action.c_str(), "createBucket" ) 
#ifdef WS_CMD_CLI_COMPAT
        || !strcmp( options.action.c_str(), "create" ) 
#endif
        )
    {
        checkBucketName( options );
        conn.createBucket( options.bucketName.c_str(), options.makePublic );
        return;
    }

    if( !strcmp( options.action.c_str(), "delBucket" ) 
#ifdef WS_CMD_CLI_COMPAT
        || !strcmp( options.action.c_str(), "delete" ) 
#endif
        )
    {
        checkBucketName( options );
        conn.delBucket( options.bucketName.c_str() );
        return;
    }

    if( !strcmp( options.action.c_str(), "listAllBuckets" ) 
#ifdef WS_CMD_CLI_COMPAT
        || !strcmp( options.action.c_str(), "list" ) 
#endif
        )
    {        
        listAllBuckets( &conn, options );
        return;
    }

    if( !strcmp( options.action.c_str(), "put" ) 
#ifdef WS_CMD_CLI_COMPAT
        || !strcmp( options.action.c_str(), "putbin" ) 
#endif
        )
    {        
        put( &conn, options, stat );
        return;
    }

    if( !strcmp( options.action.c_str(), "get" ) )
    {        
        get( &conn, options, stat );
        return;
    }

    if( !strcmp( options.action.c_str(), "del" ) )
    {
        checkBucketName( options );
        conn.del( options.bucketName.c_str(), options.prefix.c_str() );
        return;
    }

    if( !strcmp( options.action.c_str(), "delAll" ) 
#ifdef WS_CMD_CLI_COMPAT
        || !strcmp( options.action.c_str(), "delete-all-entries" )
#endif
        )
    {
        checkBucketName( options );
        conn.delAll( options.bucketName.c_str(), options.prefix.c_str() );
        return;
    }

    if( !strcmp( options.action.c_str(), "listAllObjects" ) 
#ifdef WS_CMD_CLI_COMPAT
        || !strcmp( options.action.c_str(), "entries" )
#endif
        )
    {
        listAllObjects( &conn, options );
        return;
    }

    if( !strcmp( options.action.c_str(), "listAllMultipartUploads" ) )
    {
        listAllMultipartUploads( &conn, options );
        return;
    }

    if( !strcmp( options.action.c_str(), "abortAllMultipartUploads" ) )
    {
        checkBucketName( options );
        conn.abortAllMultipartUploads( options.bucketName.c_str(), options.prefix.c_str() );
        return;
    }

    // Unknown action.

    snprintf( s_errMsg, sizeof( s_errMsg ) - 1, "Unknown action '%s'. Check the -a option.", 
        options.action.c_str() );
    throw s_errMsg;
}

int
main( int argc, char **argv )
{
    try
    {
        // Read options.

        Options options;

        if( argc <= 2 )
        {
            options.showUsage = true;
        }
        else
        {
            readEnvVars( &options );
            parseCommandLine( argc, argv, &options );
        }

        if( options.showUsage )
        {
            // Show usage.

            usage();
            return 1;
        }

        // Execute.

        Statistics stat;

        Stopwatch stopwatch( true );
        execute( options, &stat );

        UInt64 elapsed = stopwatch.elapsed();
        std::cout << "elapsed: " << elapsed << " ms";
        
        // Print statistics.

        if( stat.dataTransfered > 0 && elapsed > 0 )
        {
            std::cout << ", throughput: " << ( UInt64 )stat.dataTransfered / elapsed << " KBs";
        }

        std::cout << std::endl;

        return 0;
    }
    catch( const std::exception &e )
    {
        std::cout << std::endl << e.what() << std::endl;
    }
    catch( const char *s )
    {
        std::cout << std::endl << s << std::endl;
    }
    catch( ... )
    {
        std::cout << std::endl << "Unknown error" << std::endl;
    }

    return 1;
}
