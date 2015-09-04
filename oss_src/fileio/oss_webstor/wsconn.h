#ifndef INCLUDED_WSCONN_H
#define INCLUDED_WSCONN_H

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
// WebStor classes built on top of cloud storage REST API.
//////////////////////////////////////////////////////////////////////////////

///@mainpage  C++ library to access cloud storage.  
///
/// Provides several rare features such as multi-part upload, async support,
/// HTTP proxy, HTTP tracing.  Supports Amazon S3, Google Cloud Storage and
/// Eucalyptus Walrus. It is tuned to utilize HTTP stack efficiently,
/// and offers robust error handling. The library contains built-in SSL CA
/// certificates required to establish secure SSL connection to Amazon S3
/// and Google Cloud Storage.

#include "asyncurl.h"

#include <exception>
#include <string>
#include <vector>

namespace webstor
{

//////////////////////////////////////////////////////////////////////////////
// Debugging support.

#ifdef DEBUG
typedef bool ( dbgShowAssertFunc )( const char *file, int line, const char *msg, bool *ignoreAll );

void
dbgSetShowAssert( dbgShowAssertFunc *callback );
#endif

//////////////////////////////////////////////////////////////////////////////
///@brief   Cloud storage type.

enum WsStorType
{
    WST_S3,         /// Amazon S3
    WST_GCS,        /// Google Cloud Storage
    WST_WALRUS,     /// Eucalyptus Walrus
};


//////////////////////////////////////////////////////////////////////////////
///@brief   WebStor connection parameters.
///@details Pass an instance of WsConfig the WsConnection constructor.
///@code
/// WsConfig config = {};
/// config.accKey = ...;
/// config.secKey = ...;
/// config.isHttps = true;
/// 
/// WsConnection conn(config);
///@endcode

struct WsConfig
{
    /// Cloud storage access key.

    const char     *accKey;
    
    /// Cloud storage secret key.

    const char     *secKey;
    
    /// An optional region-specific host endpoint public cloud storage
    /// or mandatory host name for Walrus.

    /// Amazon S3:
    /// Leave it NULL/empty to connect to the US Standard region.
    /// Set to "us-west-1", to connect to US West CA region.
    /// See Amazon documentation for list of available end points.
    ///
    /// Google Cloud Storage:
    /// Leave it NULL/empty to connect to the Google Storage endpoint.
    ///
    /// Walrus or another Amazon S3-compatible storage providers:
    /// Specify a mandatory host name.

    const char     *host;

    /// Optional port name. 

    const char     *port;
   
    /// Indicates if HTTPS should be used for all requests.

    /// For public cloud storage providers it's recommended to set this field
    /// to 'true'.  For Walrus, it should be 'false' because Walrus doesn't
    /// support HTTPS.

    bool            isHttps;
    
    /// Cloud storage type.

    WsStorType      storType;
    
    /// Optional proxy with port name: "proxy:port".

    const char     *proxy;
    
    /// Optional file name containing SSL CA certificates.

    const char     *sslCertFile;
};

//////////////////////////////////////////////////////////////////////////////
///@brief   A single bucket.
///@details A collection of WsBuckets is returned from listAllBuckets(..).

struct WsBucket
{
                    WsBucket() {}
                    WsBucket( const char *_name,  const char *_creationDate ) 
                        : name( _name ), creationDate( _creationDate ) {}

    void            clear();
    std::string     name;
    std::string     creationDate;
};

inline void
WsBucket::clear()
{
    name.clear();
    creationDate.clear();
}

//////////////////////////////////////////////////////////////////////////////
///@brief Response from 'put' and 'putPart' requests.

struct WsPutResponse
{
                    WsPutResponse(): partNumber( 0 ) {}

    /// partNumber, set by 'putPart' request.

    int             partNumber;

    /// etag assigned to the object by the cloud storage provider.

    std::string     etag;
};

///@brief An abstract class to upload 'put' and 'putPart' payload.

struct WsPutRequestUploader
{
    ///@brief   A callback to upload 'put' and 'putPart' payload. 
    ///@details The method is supposed to return a number of bytes 
    /// it has written into the provided buffer @chunkBuf.
    /// If the return value is less than the chunkSize, the farther processing will be
    /// stopped.

    virtual size_t  onUpload( void *chunkBuf, size_t chunkSize ) = 0; 
};

//////////////////////////////////////////////////////////////////////////////
///@brief Response from 'get' request.

struct WsGetResponse
{
                    WsGetResponse() : loadedContentLength( -1 ), isTruncated( false ) {}

    /// Size of the loaded content, -1 means object is not found.

    size_t          loadedContentLength;  

    /// Indicates if the buffer is small and the content has been truncated.

    bool            isTruncated;

    /// Object's etag.

    std::string     etag;
};

///@brief An abstract class to download 'get' payload.

struct WsGetResponseLoader
{
    ///@brief   A callback to fetch 'get' payload. 
    ///@details The method is supposed to return a number of bytes it has read,
    /// if the return value is less than the chunkSize, the farther processing will be
    /// stopped.

    virtual size_t  onLoad( const void *chunkData, size_t chunkSize, size_t totalSizeHint ) = 0; 
};

//////////////////////////////////////////////////////////////////////////////
///@brief Response from 'del' and 'abortMultipartUpload' requests.

struct WsDelResponse
{
};

//////////////////////////////////////////////////////////////////////////////
///@brief A single cloud storage object.

struct WsObject
{
                    WsObject(): size( -1 ), isDir( false ) {}

                    WsObject( const char *_key, const char *_lastModified, const char *_etag, size_t _size, bool _isDir )
                        : key( _key ), lastModified( _lastModified ), etag( _etag ), size( _size ), isDir( _isDir ) {}

    void            clear();

    /// Object key.

    std::string     key;
    
    /// Last modified time.

    std::string     lastModified;
    
    /// Object's etag.

    std::string     etag;
    
    /// Object size.

    size_t          size;
    
    /// Indicates if this is 'directory' or not.

    bool            isDir;
};

inline void
WsObject::clear()
{
    key.clear();
    lastModified.clear();
    etag.clear();
    size = -1;
    isDir = false;
}

///@brief An abstract class to enumerate cloud storage objects.

struct WsObjectEnum
{
    /// A callback to enumerate cloud storage objects.

    virtual bool    onObject( const WsObject &object ) = 0; 
};

///@brief Response from 'listObjects' request.

struct WsListObjectsResponse
{
    ///@brief Indicates if this is the last page or not. If 'true', the response has been 
    /// truncated and there are more objects to read.

    bool            isTruncated;
    
    /// A marker to fetch the next page.

    std::string     nextMarker;
};

//////////////////////////////////////////////////////////////////////////////
///@brief Response from 'initiateMultipartUpload' request.

struct WsInitiateMultipartUploadResponse
{
    /// uploadId assigned by Amazon S3.

    std::string     uploadId;
};

//////////////////////////////////////////////////////////////////////////////
///@brief Response from 'completeMultipartUpload' request.

struct WsCompleteMultipartUploadResponse
{
    /// etag assigned to the created object.

    std::string     etag;
};

//////////////////////////////////////////////////////////////////////////////
///@brief A single multipart upload.

struct WsMultipartUpload
{
                    WsMultipartUpload(): isDir( false ) {}
  
                    WsMultipartUpload( const char *_key, const char *_uploadId, bool _isDir )
                        : key( _key ), uploadId( _uploadId ), isDir( _isDir ) {}

    void            clear();

    /// Object's key.

    std::string     key;
    
    /// uploadId.

    std::string     uploadId;
    
    /// Indicates if this is a 'directory'.

    bool            isDir;
};

inline void
WsMultipartUpload::clear()
{
    key.clear();
    uploadId.clear();
    isDir = false;
}

///@brief An abstract class to enumerate multipart uploads.

struct WsMultipartUploadEnum
{
    /// A callback to enumerate multipart uploads.

    virtual bool    onUpload( const WsMultipartUpload &upload ) = 0; 
};

///@brief Response from 'listMultipartUploads' request.

struct WsListMultipartUploadsResponse
{
    ///@brief Indicates if this is the last page or not. If 'true', the response has been 
    /// truncated and there are more uploads to read.

    bool            isTruncated;

    /// A key marker to read the next page.

    std::string     nextKeyMarker;

    /// A uploadId marker to read the next page.

    std::string     nextUploadIdMarker;
};

//////////////////////////////////////////////////////////////////////////////
///@brief WebStor HTTP tracing type.

enum TraceInfo 
{
    WS_TRACE_INFO_TEXT = 0,
    WS_TRACE_INFO_HEADER_IN,
    WS_TRACE_INFO_HEADER_OUT,
    WS_TRACE_INFO_DATA_IN,
    WS_TRACE_INFO_DATA_OUT,
    WS_TRACE_INFO_SSL_DATA_IN,
    WS_TRACE_INFO_SSL_DATA_OUT,
    WS_TRACE_INFO_END
};

///@brief A callback to read HTTP headers and body.

typedef int ( TraceCallback ) ( void *handle, TraceInfo type, unsigned char *data, size_t size,
    void *cookie );


class WsRequest;

//////////////////////////////////////////////////////////////////////////////
///@brief WsConnection to access cloud storage.
///@remark Thread-safety: the object is not thread safe.

class WsConnection
{
public:
    /// Minimum chunk size for multipart upload in MB.

    static const size_t c_multipartUploadMinPartSizeMB = 5; 

    /// Minimum chunk size for multipart upload in bytes.

    static const size_t c_multipartUploadMinPartSize = c_multipartUploadMinPartSizeMB * 1024 * 1024; 

    /// Special value that instructs to not include Cache-Control header.

    static const unsigned c_noCacheControl = -1;

    /// Constructs WsConnection.

                    WsConnection( const WsConfig &config );
    
    /// Closes WsConnection.

                    ~WsConnection();

   ///@brief Synchronously creates a bucket.
   ///@details Creates a bucket with <b>bucketName</b>.  In the case of
   /// Amazon S3 the region is inferred from the WsConfig.host parameter.
   ///@remarks see host property for details.

   void             createBucket( const char *bucketName, bool makePublic = false );

   ///@brief Synchronously deletes a bucket.
   ///@details Deletes a bucket with <b>bucketName</b>.

   void             delBucket( const char *bucketName );

   ///@brief Synchronously lists all buckets.
   ///@details Lists all buckets and 
   /// appends bucket names into the provided output vector <b>buckets</b>.

   void             listAllBuckets( std::vector< WsBucket > *buckets /* out */ );

   ///@brief Synchronously creates a cloud storage object.
   ///@details Creates an object identified by a <b>key</b> in a given <b>bucket</b> and 
   /// uploads <b>data</b>.

   void             put( const char *bucketName, const char *key, const void *data, size_t size,
                        const char *contentType = NULL, unsigned cacheMaxAge = c_noCacheControl,
                        bool makePublic = false, bool useSrvEncrypt = false,
                        WsPutResponse *response = NULL /* out */ );

   ///@brief Synchronously creates a cloud storage object.
   ///@details Creates an object identified by a <b>key</b> in a given <b>bucket</b> and 
   /// uploads data with <b>uploader</b>. 
   /// Total size of the data being uploaded must be
   /// specified in <b>totalSize</b>.

   void             put( const char *bucketName, const char *key, WsPutRequestUploader *uploader, size_t totalSize,
                        const char *contentType = NULL, unsigned cacheMaxAge = c_noCacheControl,
                        bool makePublic = false, bool useSrvEncrypt = false,
                        WsPutResponse *response = NULL /* out */ );

   ///@brief Synchronously loads a cloud storage object.
   ///@details Fetches content of an object identified by a <b>key</b> from
   /// a given <b>bucket</b> using provided <b>loader</b> object.
   /// The <b>response</b> tells whether the object is found or not and the size of the 
   /// loaded content. If <b>loadedContentLength</b> (in WsGetResponse) is set to -1, the object is missing.
   /// If <b>isTruncated</b> is set to true, the loader stopped reading the data and
   /// only a part of the content is returned.

   void             get( const char *bucketName, const char *key, 
                        WsGetResponseLoader *loader /* in */, 
                        WsGetResponse *response = NULL /* out */ );

   ///@brief Synchronously loads a cloud storage object.
   ///@details Fetches content of an object identified by a <b>key</b> from
   /// a given <b>bucket</b> and writes the content into the provided <b>buffer</b>.
   /// The <b>response</b> tells whether the object is found or not and the size of the 
   /// loaded content. If <b>loadedContentLength</b> (in WsGetResponse) is set to -1, 
   /// the object is missing.
   /// If <b>isTruncated</b> is set to true, the <b>buffer</b> is not large enough to hold the content
   /// and truncation happened.
   /// Consider using the other method that takes a <b>loader</b> object if you need to
   /// get hints about the total content length assuming the server provides it.

   void             get( const char *bucketName, const char *key, void *buffer, size_t size, 
                        WsGetResponse *response = NULL /* out */ );

   ///@brief Synchronously gets a page of cloud storage object identifiers.
   ///@details Lists up to the <b>maxKeys</b> objects (or 'directories') in a given <b>bucket</b> and 
   /// calls the provided <b>objectEnum</b> for each object name.
   /// Specify <b>prefix</b> and/or <b>marker</b> to control what objects to return and the start position.
   /// To list directories, pass the <b>delimiter</b> parameter which specifies what
   /// character (e.g. '/') is used as a directory separator.

   void             listObjects( const char *bucketName, const char *prefix,
                        const char *marker, const char *delimiter, unsigned int maxKeys,
                        WsObjectEnum *objectEnum,
                        WsListObjectsResponse *response = NULL /* out */ );

   ///@brief Synchronously gets a page of cloud storage object identifiers.
   ///@details Lists up to the <b>maxKeys</b> objects (or 'directories') in a given <b>bucket</b> and 
   /// appends object names into the provided output vector <b>objects</b>.
   /// Specify <b>prefix</b> and/or <b>marker</b> to control what objects to return and the start position.
   /// To list directories, pass the <b>delimiter</b> parameter which specifies what
   /// character (e.g. '/') is used as a directory separator.

   void             listObjects( const char *bucketName, const char *prefix,
                        const char *marker, const char *delimiter, unsigned int maxKeys,
                        std::vector< WsObject > *objects /* out */,
                        WsListObjectsResponse *response = NULL /* out */ );

   ///@brief Synchronously list cloud storage objects.
   ///@details Lists all objects (or 'directories') in a given <b>bucket</b> and 
   /// calls the provided <b>objectEnum</b> for each object name.
   /// Specify <b>prefix</b> to control what objects to return.
   /// To list directories, pass the <b>delimiter</b> parameter which specifies what
   /// character (e.g. '/') is used as a directory separator.

   void             listAllObjects( const char *bucketName, const char *prefix, 
                        const char *delimiter, WsObjectEnum *objectEnum, 
                        unsigned int maxKeysInBatch = 1000 );

   ///@brief Synchronously lists cloud storage objects.
   ///@details Lists all objects (or 'directories') in a given <b>bucket</b> and 
   /// appends object names into the provided output vector <b>objects</b>.
   /// Specify <b>prefix</b> to control what objects to return.
   /// To list directories, pass the <b>delimiter</b> parameter which specifies what
   /// character (e.g. '/') is used as a directory separator.

   void             listAllObjects( const char *bucketName, const char *prefix, 
                         const char *delimiter, std::vector< WsObject> *objects, 
                         unsigned int maxKeysInBatch = 1000 );

   ///@brief Synchronously deletes a cloud storage object.
   ///@details Deletes an object identified by a <b>key</b> from the given <b>bucket</b>.
   /// The method is no-op if the object doesn't exist.

   void             del( const char *bucketName, const char *key, WsDelResponse *response = NULL /* out */ );

   ///@brief Synchronously deletes cloud storage objects 
   ///@details Deletes all objects that match a <b>prefix</b> from the given <b>bucket</b>.
   /// The method is no-op if no objects exist.

   void             delAll( const char *bucketName, const char *prefix, unsigned int maxKeysInBatch = 1000 );

   // Multipart upload.

   ///@brief Synchronously initiates a multipart upload.
   ///@details Initiates a multipart upload of an object identified by a <b>key</b> into a given <b>bucket</b>.
   /// Returns an <b>uploadId</b> (in WsInitiateMultipartUploadResponse) that needs to be used 
   /// by subsequent putPart(..) and completeMultipartUpload(..) methods.

   void             initiateMultipartUpload( const char *bucketName, const char *key, 
                        const char *contentType = NULL, unsigned cacheMaxAge = c_noCacheControl,
                        bool makePublic = false, bool useSrvEncrypt = false,
                        WsInitiateMultipartUploadResponse *response = NULL /* out */  );

   ///@brief Synchronously uploads a single part.
   ///@details Uploads a single part with a given <b>partNumber</b> for a multipart
   /// upload identified by <b>bucketName</b>, <b>key</b> and <b>uploadId</b>. The latter (<b>uploadId</b>) is returned by 
   /// initiateMultipartUpload(..) method.
   /// <b>size</b> of the chunk being uploaded must be at least 5MB.
   /// <b>partNumber</b> starts with 1.
   
   void             putPart( const char *bucketName, const char *key, const char *uploadId, int partNumber,
                        const void *data, size_t size, WsPutResponse *response = NULL /* out */  );
    
   ///@brief Synchronously uploads a single part.
   ///@details Uploads a single part with a given <b>partNumber</b> for a multipart
   /// upload identified by <b>bucketName</b>, <b>key</b> and <b>uploadId</b>. The latter (<b>uploadId</b>) is returned by 
   /// initiateMultipartUpload(..) method.
   /// <b>partSize</b> of the chunk being uploaded must be at least 5MB.
   /// <b>partNumber</b> starts with 1.

   void             putPart( const char *bucketName, const char *key, const char *uploadId, int partNumber,
                        WsPutRequestUploader *uploader, size_t partSize, WsPutResponse *response = NULL /* out */  );

   ///@brief Synchronously commits a multipart upload.
   ///@details Commits a multipart upload consisting of parts specified in the <b>parts</b> array
   /// (<b>parts</b> is the pointer to the first element and <b>size</b> is the number of elements in the array).
   /// The multipart upload is identified by <b>bucketName</b>, <b>key</b> and <b>uploadId</b>. 
   /// The latter (<b>uploadId</b>) is returned by initiateMultipartUpload(..) method.

   void             completeMultipartUpload( const char *bucketName, const char *key, const char *uploadId, 
                        const WsPutResponse *parts, size_t size, 
                        WsCompleteMultipartUploadResponse *response = NULL /* out */ );

   ///@brief Synchronously aborts a multipart upload.
   ///@details Aborts a multipart upload identified by <b>bucketName</b>, <b>key</b> and <b>uploadId</b>. 
   /// The latter (<b>uploadId</b>) is returned by initiateMultipartUpload(..) method.

   void             abortMultipartUpload( const char *bucketName, const char *key, const char *uploadId, 
                        WsDelResponse *response = NULL /* out */ );

   ///@brief Synchronously aborts all multipart uploads.
   ///@details Aborts all multipart uploads that match <b>prefix</b> from a given <b>bucketName</b>.
   
   void             abortAllMultipartUploads( const char *bucketName, const char *prefix,
                        unsigned int maxUploadsInBatch = 1000 );

   ///@brief Synchronously gets a page of multipart uploads. 
   ///@details Lists up to the <b>maxUploads</b> multipart uploads 
   /// (or 'directories' where they are started) in a given <b>bucket</b> and 
   /// calls the provided <b>uploadEnum</b> for each upload id.
   /// Specify <b>prefix</b> and/or <b>keyMarker</b> with <b>uploadIdMarker</b> to control what uploads
   /// to return and the start position.
   /// To list directories, pass the <b>delimiter</b> parameter which specifies what
   /// character (e.g. '/') is used as a directory separator.

   void             listMultipartUploads( const char *bucketName, const char *prefix,
                        const char *keyMarker, const char *uploadIdMarker, const char *delimiter, 
                        unsigned int maxUploads,
                        WsMultipartUploadEnum *uploadEnum,
                        WsListMultipartUploadsResponse *response = NULL /* out */ );

   ///@brief Synchronously gets a page of multipart uploads.
   ///@details Lists up to the <b>maxUploads</b> multipart uploads 
   /// (or 'directories' where they are started) in a given <b>bucket</b> and 
   /// populates the provided <b>uploads</b> vector.
   /// Specify <b>prefix</b> and/or <b>keyMarker</b> with <b>uploadIdMarker</b> to control what uploads
   /// to return and the start position.
   /// To list directories, pass the <b>delimiter</b> parameter which specifies what
   /// character (e.g. '/') is used as a directory separator.

   void             listMultipartUploads( const char *bucketName, const char *prefix,
                        const char *keyMarker, const char *uploadIdMarker, const char *delimiter, 
                        unsigned int maxUploads,
                        std::vector< WsMultipartUpload > *uploads /* out */,
                        WsListMultipartUploadsResponse *response = NULL /* out */ );

   ///@brief Synchronously lists multipart uploads.
   ///@details Lists all uploads (or 'directories' where they are started) 
   /// in a given <b>bucket</b> and  calls the provided <b>uploadEnum</b> object.
   /// Specify <b>prefix</b> to control what uploads to return.
   /// To list directories, pass the <b>delimiter</b> parameter which specifies what
   /// character (e.g. '/') is used as a directory separator.

   void             listAllMultipartUploads( const char *bucketName, const char *prefix, 
                       const char *delimiter, WsMultipartUploadEnum *uploadEnum, 
                       unsigned int maxUploadsInBatch = 1000 );

   ///@brief Synchronously lists multipart uploads.
   ///@details Lists all uploads (or 'directories' where they are started) 
   /// in a given <b>bucket</b> and appends them to the provided <b>uploads</b> vector.
   /// Specify <b>prefix</b> to control what uploads to return.
   /// To list directories, pass the <b>delimiter</b> parameter which specifies what
   /// character (e.g. '/') is used as a directory separator.

   void             listAllMultipartUploads( const char *bucketName, const char *prefix, 
                       const char *delimiter, std::vector< WsMultipartUpload > *uploads, 
                       unsigned int maxUploadsInBatch = 1000 );

   // Async support.
   
   ///@brief Starts asynchronous <b>put</b> request.
   ///@details Asynchronously creates cloud storage object identified by a <b>key</b>
   /// in a given <b>bucket</b> and uploads <b>data</b>.
   /// Both <b>asyncMan</b> and the <b>data</b> buffer that holds data being uploaded
   /// must be available till the completePut(..) or cancelAsync(..) methods are called.
   /// Only one async operation can be started with a given WsConnection instance.
   /// If you need to start another one, call complete or cancel first. Or use
   /// multiple WsConnections.
   
   void             pendPut( AsyncMan *asyncMan, const char *bucketName, const char *key, 
                        const void *data, size_t size,
                        bool makePublic = false, bool useSrvEncrypt = false );

   ///@brief Waits and completes the asynchronous <b>put</b> request.
   ///@details Completes the started asynchronous put operation. The method blocks till the operation finishes.
   /// After the method returns, the caller can start another sync or async operation.

   void             completePut( WsPutResponse *response = NULL /* out */ );

   ///@brief Starts asynchronous <b>get</b> request.
   ///@details Asynchronously fetches content of a cloud storage object identified by
   /// a <b>key</b> from a given <b>bucket</b> and writes the content into the provided
   /// <b>buffer</b>.
   /// Both <b>asyncMan</b> and the <b>buffer</b> that holds data being downloaded
   /// must be available till the completeGet(..) or cancelAsync(..) methods are called.
   /// Only one async operation can be started with a given WsConnection instance.
   /// If you need to start another one, call complete or cancel first. Or use
   /// multiple WsConnections.

   void             pendGet( AsyncMan *asyncMan, const char *bucketName, const char *key, 
                        void *buffer, size_t size );

   ///@brief Waits and completes the asynchronous <b>get</b> request.
   ///@details Completes the started asynchronous get operation. The method blocks till the operation finishes.
   /// After the method returns, the caller can start another sync or async operation.
   /// The <b>response</b> tells whether the object is found or not and the size of the 
   /// loaded content. 
   /// If <b>loadedContentLength</b> (in WsGetResponse) is set to -1, 
   /// the object is missing.
   /// If <b>isTruncated</b> is set to true, the <b>buffer</b> is not large enough to hold the content
   /// and truncation happened.
   void             completeGet( WsGetResponse *response = NULL /* out */ );

   ///@brief Starts asynchronous <b>del</b> request.
   ///@details Asynchronously deletes a cloud storage object identified by a <b>key</b> from
   /// a given <b>bucket</b>.
   /// The <b>asyncMan</b> must be available till the completeDel(..) or cancelAsync(..) 
   /// methods are called.

   void             pendDel( AsyncMan *asyncMan, const char *bucketName, const char *key );

   ///@brief Waits and completes the asynchronous <b>del</b> request.
   ///@details Completes the started asynchronous del operation. The method blocks till the operation finishes.
   /// After the method returns, the caller can start another sync or async operation.

   void             completeDel( WsDelResponse *response = NULL /* out */ );

   /// Returns true if an async operation is in progress.

   bool             isAsyncPending(); // nofail   

   /// Returns true if an async operation is completed.

   bool             isAsyncCompleted(); // nofail

   /// Cancel any pending async operations.

   void             cancelAsync(); // nofail

   /// Maximum number of WsConnection's waitAny(..) supports.

   enum { c_maxWaitAny = 64 };

   ///@brief Waits for any WsConnection to complete async operation, returns -1 if timeout.
   ///@details <b>startFrom</b> specifies connection index to start the check from.
   /// The caller can change this index in round-robin fashion to ensure fairness.
   /// <b>count</b> specifies number of connections in <b>cons</b> array. This number should be
   /// less or equal to c_maxWaitAny.

   static int       waitAny( WsConnection **cons, size_t count, size_t startFrom = 0,
                            long timeout = -1 /* infinite */ );

   /// Sets timeouts, in milliseconds.

   void             setTimeout( long timeout ); 

   /// Sets connect timeouts, in milliseconds.

   void             setConnectTimeout( long connectTime ); 

   /// Enables HTTP tracing.

   void             enableTracing( TraceCallback *traceCallback ) { m_traceCallback = traceCallback; }  // nofail

private:
                    WsConnection( const WsConnection & );  // forbidden
     WsConnection & operator=( const WsConnection & );  // forbidden

    void            prepare( WsRequest *request, const char *bucketName, const char *key,
                        const char *contentType = NULL, unsigned cacheMaxAge = c_noCacheControl,
                        bool makePublic = false, bool useSrvEncrypt = false );

    void            init( WsRequest *request, const char *bucketName, const char *key, 
                        const char *keySuffix = NULL, const char *contentType = NULL, unsigned cacheMaxAge = c_noCacheControl,
                        bool makePublic = false, bool useSrvEncrypt = false );

    void            put( WsRequest *request, const char *bucketName, const char *key, 
                        const char *uploadId, int partNumber, 
                        const char *contentType, unsigned cacheMaxAge,
                        bool makePublic, bool useSrvEncrypt,
                        WsPutResponse *response );

    void            del( const char *bucketName, const char *key, const char *keySuffix, 
                        WsDelResponse *response );

    std::string     m_accKey;
    std::string     m_secKey;
    std::string     m_baseUrl;
    std::string     m_region;
    WsStorType      m_storType;
    bool            m_isHttps;
    std::string     m_proxy;
    std::string     m_sslCertFile;

    char            m_errorBuffer[ 256 ];
    TraceCallback * m_traceCallback;

    internal::AsyncCurl m_curl;

    // Async support.

    WsRequest *     m_asyncRequest;

    // Timeouts.

    long            m_timeout;          // in milliseconds
    long            m_connectTimeout;   // in milliseconds
};

}  // namespace webstor

#endif // !INCLUDED_WSCONN_H
