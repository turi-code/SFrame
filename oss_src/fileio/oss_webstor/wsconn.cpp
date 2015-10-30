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
// WebStor connection.
//////////////////////////////////////////////////////////////////////////////

#include "wsconn.h"

#include "sysutils.h"

#define NOMINMAX
#include <curl/curl.h>
#include <libxml2/libxml/parser.h>
#include <openssl/err.h>
#include <openssl/hmac.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>
#include <fileio/set_curl_ssl_options.hpp>
#include <algorithm>
#include <memory>
#include <fileio/fileio_constants.hpp>

#pragma GCC diagnostic ignored "-Wreorder"
#pragma GCC diagnostic ignored "-Wsign-compare"
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
namespace webstor
{

using namespace internal;

//////////////////////////////////////////////////////////////////////////////
// WebStor errors.

const static char errUnexpected[] = "Unexpected error." ;
const static char errCurl[] = "%s.";
const static char errHTTP[] = "%s.";
const static char errHTTPResourceNotFound[] = "HTTP resource not found: %s."; 
const static char errParser[] = "Cannot parse the response.";
const static char errWsDetail[] = "%s (Code='%s', RequestId='%s')."; 
const static char errOpSummary[] = "The '%s' operation for '%s' failed. %s";
const static char errTooManyConnetions[] = "Too many connections passed to waitAny method.";

//////////////////////////////////////////////////////////////////////////////
// WebStor statics.

static const char s_defaultS3Host[] = "s3.amazonaws.com";
static const char s_defaultGcsHost[] = "commondatastorage.googleapis.com";
static const char s_defaultWalrusPort[] = "8773";
static const char s_CACertIgnore[] = "none";
static const char s_contentTypeBinary[] = "application/octet-stream";
static const char s_contentTypeXml[] = "application/xml";

// Default timeouts.

// We need default timeouts, otherwise WsConnection may stuck forever if cable is unplugged or
// anything else happens stopping any activity on socket(s).

static const UInt32 s_defaultTimeout = 120 * 1000;          // 2 mins
static const UInt32 s_defaultConnectTimeout = 30 * 1000;    // 30 secs

// Enable TCP KeepAlive to let TCP stack send keepalive probes
// when transfer is idle. This allows detecting connection issues faster.

static const TcpKeepAliveParams s_tcpKeepAliveProbes = 
{ 
    // The following parameters should allow detecting connection issues within 20 secs:
    // probeStartTime + probeIntervalTime * probeCount

    5 * 1000,   // 5 sec, how long to stay in idle before sending keepalive probe, in milliseconds
    5 * 1000,   // 5 sec, delay between TCP keepalive probes, in milliseconds
    3           // 3, the number of unacknowledged probes to send before considering the connection dead
                // The latter is not supported on Windows.  See webstor::internal::setTcpKeepAlive(..).
};

// Maximum socket send/receive buffer size.
// This gives us: throughput = window_size / RTT = 1MB / 100 ms = 10 MB/s on one connection
// Note: On Linux, the  kernel doubles this value for internal bookkeeping overhead.
// Note: the OS may limit the buffer size according to the system-wide settings.  To
// take advantage of high-bandwidth & high-latency network, the receive buffer size
// must be large enough to support large TCP window sizes.  The default max value on
// Linux (as of 2012/09/15) is 128 KB, which is too small.  The value is controlled
// by /proc/sys/net/core/rmem_max which should be set to at least 2 MB.

static const UInt32 s_socketBufferSize = 1024 * 1024;  // 1MB

//////////////////////////////////////////////////////////////////////////////
// String conversion functions. 

#ifdef _WIN32
static inline Int64 atoll( const char *s ) { return _atoi64(s); }
#endif

static const char *uitoa( unsigned int val, char *buf )
{   
    char *p = buf;    
    
    do
    {     
        *p++ = '0' + val % 10;    
        val /= 10;
    } while( val > 0 );

    *p = '\0';
    p--;

    // Swap.
    
    for( char *p1 = buf; p1 < p; ++p1, --p )
    {
        char tmp = *p;
        *p = *p1;
        *p1 = tmp;
    }
    
    return buf;
}

//////////////////////////////////////////////////////////////////////////////
// Default SSL certificates.

static const char 
**getDefaultCACerts()
{
    // AWS SSL Server Certificates have been issued by several CAs
    // depending on AWS region. E.g. standard US is issued by
    // "VeriSign Class 3 Secure Server CA - G2".
    // This and other certificates are hardcoded here.

    // The certificate chain used by AWS can be verified using the following
    // command: 
    //    openssl s_client -connect <region-specific-url>:443,
    // example:
    //    openssl s_client -connect s3.amazonaws.com:443

    static const char *s_certs[] =
    {
        // VeriSign Class 3 Public Primary Certification Authority - G5
        // * US Standard (s3.amazonaws.com:443)

        "-----BEGIN CERTIFICATE-----\n"
        "MIIE0zCCA7ugAwIBAgIQGNrRniZ96LtKIVjNzGs7SjANBgkqhkiG9w0BAQUFADCByjELMAkGA1UE\n"
        "BhMCVVMxFzAVBgNVBAoTDlZlcmlTaWduLCBJbmMuMR8wHQYDVQQLExZWZXJpU2lnbiBUcnVzdCBO\n"
        "ZXR3b3JrMTowOAYDVQQLEzEoYykgMjAwNiBWZXJpU2lnbiwgSW5jLiAtIEZvciBhdXRob3JpemVk\n"
        "IHVzZSBvbmx5MUUwQwYDVQQDEzxWZXJpU2lnbiBDbGFzcyAzIFB1YmxpYyBQcmltYXJ5IENlcnRp\n"
        "ZmljYXRpb24gQXV0aG9yaXR5IC0gRzUwHhcNMDYxMTA4MDAwMDAwWhcNMzYwNzE2MjM1OTU5WjCB\n"
        "yjELMAkGA1UEBhMCVVMxFzAVBgNVBAoTDlZlcmlTaWduLCBJbmMuMR8wHQYDVQQLExZWZXJpU2ln\n"
        "biBUcnVzdCBOZXR3b3JrMTowOAYDVQQLEzEoYykgMjAwNiBWZXJpU2lnbiwgSW5jLiAtIEZvciBh\n"
        "dXRob3JpemVkIHVzZSBvbmx5MUUwQwYDVQQDEzxWZXJpU2lnbiBDbGFzcyAzIFB1YmxpYyBQcmlt\n"
        "YXJ5IENlcnRpZmljYXRpb24gQXV0aG9yaXR5IC0gRzUwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAw\n"
        "ggEKAoIBAQCvJAgIKXo1nmAMqudLO07cfLw8RRy7K+D+KQL5VwijZIUVJ/XxrcgxiV0i6CqqpkKz\n"
        "j/i5Vbext0uz/o9+B1fs70PbZmIVYc9gDaTY3vjgw2IIPVQT60nKWVSFJuUrjxuf6/WhkcIzSdhD\n"
        "Y2pSS9KP6HBRTdGJaXvHcPaz3BJ023tdS1bTlr8Vd6Gw9KIl8q8ckmcY5fQGBO+QueQA5N06tRn/\n"
        "Arr0PO7gi+s3i+z016zy9vA9r911kTMZHRxAy3QkGSGT2RT+rCpSx4/VBEnkjWNHiDxpg8v+R70r\n"
        "fk/Fla4OndTRQ8Bnc+MUCH7lP59zuDMKz10/NIeWiu5T6CUVAgMBAAGjgbIwga8wDwYDVR0TAQH/\n"
        "BAUwAwEB/zAOBgNVHQ8BAf8EBAMCAQYwbQYIKwYBBQUHAQwEYTBfoV2gWzBZMFcwVRYJaW1hZ2Uv\n"
        "Z2lmMCEwHzAHBgUrDgMCGgQUj+XTGoasjY5rw8+AatRIGCx7GS4wJRYjaHR0cDovL2xvZ28udmVy\n"
        "aXNpZ24uY29tL3ZzbG9nby5naWYwHQYDVR0OBBYEFH/TZafC3ey78DAJ80M5+gKvMzEzMA0GCSqG\n"
        "SIb3DQEBBQUAA4IBAQCTJEowX2LP2BqYLz3q3JktvXf2pXkiOOzEp6B4Eq1iDkVwZMXnl2YtmAl+\n"
        "X6/WzChl8gGqCBpH3vn5fJJaCGkgDdk+bW48DW7Y5gaRQBi5+MHt39tBquCWIMnNZBU4gcmU7qKE\n"
        "KQsTb47bDN0lAtukixlE0kF6BWlKWE9gyn6CagsCqiUXObXbf+eEZSqVir2G3l6BFoMtEMze/aiC\n"
        "Km0oHw0LxOXnGiYZ4fQRbxC1lfznQgUy286dUV4otp6F01vvpX1FQHKOtw5rDgb7MzVIcbidJ4vE\n"
        "ZV8NhnacRHr2lVz2XTIIM6RUthg/aFzyQkqFOFSDX9HoLPKsEdao7WNq\n"
        "-----END CERTIFICATE-----\n",

        // Verisign Class 3 Public Primary Certification Authority - G2.
        // * US Standard (s3.amazonaws.com:443) - before 10/02/2013

        "-----BEGIN CERTIFICATE-----\n"
        "MIIDAjCCAmsCEH3Z/gfPqB63EHln+6eJNMYwDQYJKoZIhvcNAQEFBQAwgcExCzAJBgNVBAYTAlVT\n"
        "MRcwFQYDVQQKEw5WZXJpU2lnbiwgSW5jLjE8MDoGA1UECxMzQ2xhc3MgMyBQdWJsaWMgUHJpbWFy\n"
        "eSBDZXJ0aWZpY2F0aW9uIEF1dGhvcml0eSAtIEcyMTowOAYDVQQLEzEoYykgMTk5OCBWZXJpU2ln\n"
        "biwgSW5jLiAtIEZvciBhdXRob3JpemVkIHVzZSBvbmx5MR8wHQYDVQQLExZWZXJpU2lnbiBUcnVz\n"
        "dCBOZXR3b3JrMB4XDTk4MDUxODAwMDAwMFoXDTI4MDgwMTIzNTk1OVowgcExCzAJBgNVBAYTAlVT\n"
        "MRcwFQYDVQQKEw5WZXJpU2lnbiwgSW5jLjE8MDoGA1UECxMzQ2xhc3MgMyBQdWJsaWMgUHJpbWFy\n"
        "eSBDZXJ0aWZpY2F0aW9uIEF1dGhvcml0eSAtIEcyMTowOAYDVQQLEzEoYykgMTk5OCBWZXJpU2ln\n"
        "biwgSW5jLiAtIEZvciBhdXRob3JpemVkIHVzZSBvbmx5MR8wHQYDVQQLExZWZXJpU2lnbiBUcnVz\n"
        "dCBOZXR3b3JrMIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQDMXtERXVxp0KvTuWpMmR9ZmDCO\n"
        "FoUgRm1HP9SFIIThbbP4pO0M8RcPO/mn+SXXwc+EY/J8Y8+iR/LGWzOOZEAEaMGAuWQcRXfH2G71\n"
        "lSk8UOg013gfqLptQ5GVj0VXXn7F+8qkBOvqlzdUMG+7AUcyM83cV5tkaWH4mx0ciU9cZwIDAQAB\n"
        "MA0GCSqGSIb3DQEBBQUAA4GBAFFNzb5cy5gZnBWyATl4Lk0PZ3BwmcYQWpSkU01UbSuvDV1Ai2TT\n"
        "1+7eVmGSX6bEHRBhNtMsJzzoKQm5EWR0zLVznxxIqbxhAe7iF6YM40AIOw7n60RzKprxaZLvcRTD\n"
        "Oaxxp5EJb+RxBrO6WVcmeQD2+A2iMzAo1KpYoJ2daZH9\n"
        "-----END CERTIFICATE-----\n",

        // Entrust.net Secure Server CA
        // * US West-1 N. California (s3-us-west-2.amazonaws.com)
        // * US West-2 Oregon (s3-us-west-1.amazonaws.com)

        "-----BEGIN CERTIFICATE-----\n"
        "MIIE2DCCBEGgAwIBAgIEN0rSQzANBgkqhkiG9w0BAQUFADCBwzELMAkGA1UEBhMCVVMxFDASBgNV\n"
        "BAoTC0VudHJ1c3QubmV0MTswOQYDVQQLEzJ3d3cuZW50cnVzdC5uZXQvQ1BTIGluY29ycC4gYnkg\n"
        "cmVmLiAobGltaXRzIGxpYWIuKTElMCMGA1UECxMcKGMpIDE5OTkgRW50cnVzdC5uZXQgTGltaXRl\n"
        "ZDE6MDgGA1UEAxMxRW50cnVzdC5uZXQgU2VjdXJlIFNlcnZlciBDZXJ0aWZpY2F0aW9uIEF1dGhv\n"
        "cml0eTAeFw05OTA1MjUxNjA5NDBaFw0xOTA1MjUxNjM5NDBaMIHDMQswCQYDVQQGEwJVUzEUMBIG\n"
        "A1UEChMLRW50cnVzdC5uZXQxOzA5BgNVBAsTMnd3dy5lbnRydXN0Lm5ldC9DUFMgaW5jb3JwLiBi\n"
        "eSByZWYuIChsaW1pdHMgbGlhYi4pMSUwIwYDVQQLExwoYykgMTk5OSBFbnRydXN0Lm5ldCBMaW1p\n"
        "dGVkMTowOAYDVQQDEzFFbnRydXN0Lm5ldCBTZWN1cmUgU2VydmVyIENlcnRpZmljYXRpb24gQXV0\n"
        "aG9yaXR5MIGdMA0GCSqGSIb3DQEBAQUAA4GLADCBhwKBgQDNKIM0VBuJ8w+vN5Ex/68xYMmo6LIQ\n"
        "aO2f55M28Qpku0f1BBc/I0dNxScZgSYMVHINiC3ZH5oSn7yzcdOAGT9HZnuMNSjSuQrfJNqc1lB5\n"
        "gXpa0zf3wkrYKZImZNHkmGw6AIr1NJtl+O3jEP/9uElY3KDegjlrgbEWGWG5VLbmQwIBA6OCAdcw\n"
        "ggHTMBEGCWCGSAGG+EIBAQQEAwIABzCCARkGA1UdHwSCARAwggEMMIHeoIHboIHYpIHVMIHSMQsw\n"
        "CQYDVQQGEwJVUzEUMBIGA1UEChMLRW50cnVzdC5uZXQxOzA5BgNVBAsTMnd3dy5lbnRydXN0Lm5l\n"
        "dC9DUFMgaW5jb3JwLiBieSByZWYuIChsaW1pdHMgbGlhYi4pMSUwIwYDVQQLExwoYykgMTk5OSBF\n"
        "bnRydXN0Lm5ldCBMaW1pdGVkMTowOAYDVQQDEzFFbnRydXN0Lm5ldCBTZWN1cmUgU2VydmVyIENl\n"
        "cnRpZmljYXRpb24gQXV0aG9yaXR5MQ0wCwYDVQQDEwRDUkwxMCmgJ6AlhiNodHRwOi8vd3d3LmVu\n"
        "dHJ1c3QubmV0L0NSTC9uZXQxLmNybDArBgNVHRAEJDAigA8xOTk5MDUyNTE2MDk0MFqBDzIwMTkw\n"
        "NTI1MTYwOTQwWjALBgNVHQ8EBAMCAQYwHwYDVR0jBBgwFoAU8BdiE1U9s/8KAGv7UISX8+1i0Bow\n"
        "HQYDVR0OBBYEFPAXYhNVPbP/CgBr+1CEl/PtYtAaMAwGA1UdEwQFMAMBAf8wGQYJKoZIhvZ9B0EA\n"
        "BAwwChsEVjQuMAMCBJAwDQYJKoZIhvcNAQEFBQADgYEAkNwwAvpkdMKnCqV8IY00F6j7Rw7/JXyN\n"
        "Ewr75Ji174z4xRAN95K+8cPV1ZVqBLssziY2ZcgxxufuP+NXdYR6Ee9GTxj005i7qIcyunL2POI9\n"
        "n9cd2cNgQ4xYDiKWL2KjLB+6rQXvqzJ4h6BUcxm1XAX5Uj5tLUUL9wqT6u0G+bI=\n"
        "-----END CERTIFICATE-----\n",

        // DigiCert High Assurance EV Root CA
        // * EU Ireland (s3-eu-west-1.amazonaws.com)
        // * Asia Pacific Singapore (s3-ap-southeast-1.amazonaws.com)
        // * Asia Pacific Tokyo (s3-ap-northeast-1.amazonaws.com)
        // * South America (Sao Paulo) Region (s3-sa-east-1.amazonaws.com)

        "-----BEGIN CERTIFICATE-----\n"
        "MIIDxTCCAq2gAwIBAgIQAqxcJmoLQJuPC3nyrkYldzANBgkqhkiG9w0BAQUFADBsMQswCQYDVQQG\n"
        "EwJVUzEVMBMGA1UEChMMRGlnaUNlcnQgSW5jMRkwFwYDVQQLExB3d3cuZGlnaWNlcnQuY29tMSsw\n"
        "KQYDVQQDEyJEaWdpQ2VydCBIaWdoIEFzc3VyYW5jZSBFViBSb290IENBMB4XDTA2MTExMDAwMDAw\n"
        "MFoXDTMxMTExMDAwMDAwMFowbDELMAkGA1UEBhMCVVMxFTATBgNVBAoTDERpZ2lDZXJ0IEluYzEZ\n"
        "MBcGA1UECxMQd3d3LmRpZ2ljZXJ0LmNvbTErMCkGA1UEAxMiRGlnaUNlcnQgSGlnaCBBc3N1cmFu\n"
        "Y2UgRVYgUm9vdCBDQTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAMbM5XPm+9S75S0t\n"
        "Mqbf5YE/yc0lSbZxKsPVlDRnogocsF9ppkCxxLeyj9CYpKlBWTrT3JTWPNt0OKRKzE0lgvdKpVMS\n"
        "OO7zSW1xkX5jtqumX8OkhPhPYlG++MXs2ziS4wblCJEMxChBVfvLWokVfnHoNb9Ncgk9vjo4UFt3\n"
        "MRuNs8ckRZqnrG0AFFoEt7oT61EKmEFBIk5lYYeBQVCmeVyJ3hlKV9Uu5l0cUyx+mM0aBhakaHPQ\n"
        "NAQTXKFx01p8VdteZOE3hzBWBOURtCmAEvF5OYiiAhF8J2a3iLd48soKqDirCmTCv2ZdlYTBoSUe\n"
        "h10aUAsgEsxBu24LUTi4S8sCAwEAAaNjMGEwDgYDVR0PAQH/BAQDAgGGMA8GA1UdEwEB/wQFMAMB\n"
        "Af8wHQYDVR0OBBYEFLE+w2kD+L9HAdSYJhoIAu9jZCvDMB8GA1UdIwQYMBaAFLE+w2kD+L9HAdSY\n"
        "JhoIAu9jZCvDMA0GCSqGSIb3DQEBBQUAA4IBAQAcGgaX3NecnzyIZgYIVyHbIUf4KmeqvxgydkAQ\n"
        "V8GK83rZEWWONfqe/EW1ntlMMUu4kehDLI6zeM7b41N5cdblIZQB2lWHmiRk9opmzN6cN82oNLFp\n"
        "myPInngiK3BD41VHMWEZ71jFhS9OMPagMRYjyOfiZRYzy78aG6A9+MpeizGLYAiJLQwGXFK3xPkK\n"
        "mNEVX58Svnw2Yzi9RKR/5CYrCsSXaQ3pjOLAEFe4yHYSkVXySGnYvCoCWw9E1CAx2/S6cCZdkGCe\n"
        "vEsXCS+0yx5DaMkHJ8HSXPfqIbloEpw8nL+e/IBcm2PN7EeqJSdnoDfzAIJ9VNep+OkuE6N36B9K\n"
        "-----END CERTIFICATE-----",

        // Equifax Secure CA
        // * Google Cloud Storage (commondatastorage.googleapis.com)

        "-----BEGIN CERTIFICATE-----\n"
        "MIIDIDCCAomgAwIBAgIENd70zzANBgkqhkiG9w0BAQUFADBOMQswCQYDVQQGEwJVUzEQMA4GA1UE\n"
        "ChMHRXF1aWZheDEtMCsGA1UECxMkRXF1aWZheCBTZWN1cmUgQ2VydGlmaWNhdGUgQXV0aG9yaXR5\n"
        "MB4XDTk4MDgyMjE2NDE1MVoXDTE4MDgyMjE2NDE1MVowTjELMAkGA1UEBhMCVVMxEDAOBgNVBAoT\n"
        "B0VxdWlmYXgxLTArBgNVBAsTJEVxdWlmYXggU2VjdXJlIENlcnRpZmljYXRlIEF1dGhvcml0eTCB\n"
        "nzANBgkqhkiG9w0BAQEFAAOBjQAwgYkCgYEAwV2xWGcIYu6gmi0fCG2RFGiYCh7+2gRvE4RiIcPR\n"
        "fM6fBeC4AfBONOziipUEZKzxa1NfBbPLZ4C/QgKO/t0BCezhABRP/PvwDN1Dulsr4R+AcJkVV5MW\n"
        "8Q+XarfCaCMczE1ZMKxRHjuvK9buY0V7xdlfUNLjUA86iOe/FP3gx7kCAwEAAaOCAQkwggEFMHAG\n"
        "A1UdHwRpMGcwZaBjoGGkXzBdMQswCQYDVQQGEwJVUzEQMA4GA1UEChMHRXF1aWZheDEtMCsGA1UE\n"
        "CxMkRXF1aWZheCBTZWN1cmUgQ2VydGlmaWNhdGUgQXV0aG9yaXR5MQ0wCwYDVQQDEwRDUkwxMBoG\n"
        "A1UdEAQTMBGBDzIwMTgwODIyMTY0MTUxWjALBgNVHQ8EBAMCAQYwHwYDVR0jBBgwFoAUSOZo+SvS\n"
        "spXXR9gjIBBPM5iQn9QwHQYDVR0OBBYEFEjmaPkr0rKV10fYIyAQTzOYkJ/UMAwGA1UdEwQFMAMB\n"
        "Af8wGgYJKoZIhvZ9B0EABA0wCxsFVjMuMGMDAgbAMA0GCSqGSIb3DQEBBQUAA4GBAFjOKer89961\n"
        "zgK5F7WF0bnj4JXMJTENAKaSbn+2kmOeUJXRmm/kEd5jhW6Y7qj/WsjTVbJmcVfewCHrPSqnI0kB\n"
        "BIZCe/zuf6IWUrVnZ9NA2zsmWLIodz2uFHdh1voqZiegDfqnc1zqcPGUIWVEX/r87yloqaKHee95\n"
        "70+sB3c4\n"
        "-----END CERTIFICATE-----\n",

        NULL
    };

    return s_certs;
}

struct BIODeleter
{
    static void     free( BIO *bio ){ dbgAssert( bio ); BIO_free_all( bio ); }
};

struct X509Deleter
{
    static void     free( X509 *cert ){ dbgAssert( cert ); X509_free( cert ); }
};

static CURLcode 
addDefaultCACerts( CURL * curl, void * sslctx, void * parm ) // nofail
{
    // Get X509 certificate store.

    X509_STORE* store = SSL_CTX_get_cert_store( reinterpret_cast< SSL_CTX * >( sslctx ) );

    for ( const char **p = getDefaultCACerts(); *p != NULL; ++p ) 
    {
        // Construct a BIO.
        
        BIO* bio = BIO_new_mem_buf( ( void *)( *p ), -1 );

        if( !bio )
        {
            return CURLE_OUT_OF_MEMORY;
        }

        auto_scope< BIO *, BIODeleter > scoped_bio( bio );

        // Extract a certificate.

        X509* cert = PEM_read_bio_X509( bio, NULL, 0, NULL );

        if( !cert ) 
        {
            dbgPanicSz( "UNEXPECTED: Cannot read the default root certificate!!!" ); 
            continue;
        }

        auto_scope< X509 *, X509Deleter > scoped_cert( cert );

        // Add it to the store.

        bool isAdded = X509_STORE_add_cert( store, cert );
        unsigned long err = ERR_get_error();

        if( !isAdded && 
            !( ERR_GET_LIB( err ) == ERR_LIB_X509 && 
            ERR_GET_REASON( err ) == X509_R_CERT_ALREADY_IN_HASH_TABLE ) ) 
        {
                dbgPanicSz( "UNEXPECTED: Cannot add the default root certificate!!!" ); 
                continue;
        }
    }

    return CURLE_OK;
}

//////////////////////////////////////////////////////////////////////////////
// Common utils.

static void
append64Encoded( std::string *encoded, const void *data, size_t size )
{
    dbgAssert( implies( size, data ) );
    dbgAssert( encoded );

    if( !size )
    {
        return;
    }

    auto_scope< BIO *, BIODeleter > bio( BIO_new( BIO_s_mem() ) );

    if( !bio )
    {
        throw std::bad_alloc();
    }

    BIO* base64 = BIO_new( BIO_f_base64() );

    if( !base64 )
    {
        throw std::bad_alloc();
    }

    BIO_set_flags( base64, BIO_FLAGS_BASE64_NO_NL );
    bio.reset( BIO_push( base64, bio.release() ) ); // bio will free all objects
    dbgAssert( bio );

    dbgVerify( BIO_write( bio, data, size ) > 0 );
    dbgVerify( BIO_flush( bio ) > 0 );

    const char *tmp = NULL;
    size_t length = BIO_get_mem_data( bio, &tmp );
    dbgAssert( tmp && length );

    encoded->append( tmp, length );
}

struct CurlStringDeleter
{
    static void     free( char *value ){ dbgAssert( value ); curl_free( value ); }
};

static void 
appendEscapedUrl( std::string *escapedUrl, const char *value )
{
    dbgAssert( value );
    dbgAssert( escapedUrl );

    char *escapedValue = curl_escape( value, 0 /* auto detect length */ );

    if( !escapedValue )
    {
        // This can be either OOM or unsupported character. We control what
        // strings we escape (except for weblob extensions but that should be
        // an extreme case) so if we are here it's most likely OOM.

        throw std::bad_alloc();
    }

    auto_scope< char *, CurlStringDeleter > scoped( escapedValue );
    escapedUrl->append( escapedValue );
}

#define curl_easy_setopt_checked( handle, option, ... ) dbgVerify( curl_easy_setopt( handle, option, __VA_ARGS__ ) == CURLE_OK )

//////////////////////////////////////////////////////////////////////////////
// Helper methods to deal with the query part of url.

static void
appendQueryPart( std::string *url, const char *key, const char *value, bool *pfirst = 0 /* in / out */ )
{
    dbgAssert( url );
    dbgAssert( key );

    if( !value )
        return;

    // Append "?[key]=[value]" or "&[key]=[value]" to the URL.

    // The value is escaped, the key is supposed to not require
    // escaping.

    url->append( 1, pfirst && *pfirst ? '?' : '&' );
    url->append( key );
    url->append( 1, '=' );
    appendEscapedUrl( url, value );

    if( pfirst )
        *pfirst = false;
}

//////////////////////////////////////////////////////////////////////////////
// Helper methods for signing.

static const char s_aclHeaderKey[] = "x-amz-acl";
static const char s_aclHeaderValue[] = "public-read";

static const char s_encryptHeaderKey[] = "x-amz-server-side-encryption";
static const char s_encryptHeaderValue[] = "AES256";

static inline void
appendSigHeader( const char *key, const char *value, std::string *ptoSign )
{
    dbgAssert( ptoSign );

    // Some headers include key and some headers contains only value.

    if( key )
    {
        ptoSign->append( key );
        ptoSign->append( 1, ':' );
    }

    ptoSign->append( value ? value : "" );
    ptoSign->append( 1, '\n' );
}

static void
calcSignature( const std::string &accKey, const std::string &secKey,
    const char *contentMd5, const char *contentType, const char *date, bool makePublic, bool srvEncrypt,
    const char *action, const char *bucketName, const char *key, WsStorType storType, 
    std::string *signature )
{
    dbgAssert( action );
    dbgAssert( signature );

    // Construct a string to sign.

    std::string toSign;
    toSign.reserve( 1024 );
    toSign.append( action );
    toSign.append( 1, '\n' );

    // Add headers.

    appendSigHeader( 0, contentMd5, &toSign );
    appendSigHeader( 0, contentType, &toSign );
    appendSigHeader( 0, date, &toSign );

    if( makePublic )
        appendSigHeader( s_aclHeaderKey, s_aclHeaderValue, &toSign );

    if( srvEncrypt )
        appendSigHeader( s_encryptHeaderKey, s_encryptHeaderValue, &toSign );

    // Add URL.

    if( storType == WST_WALRUS )
        toSign.append( STRING_WITH_LEN( "/services/Walrus" ) );

    if( bucketName )
    { 
        toSign.append( 1, '/' );
        toSign.append( bucketName );
    }

    if( key )
    {
        toSign.append( 1, '/' );
        toSign.append( key );
    }       

    // Compute signature.

    unsigned char   hash[ 1024 ];
    unsigned int    hashSize;

    HMAC( EVP_sha1(), secKey.c_str(),  secKey.size(),
        reinterpret_cast< const unsigned char* >( toSign.c_str() ), toSign.size(),
        hash, &hashSize );

    signature->append( STRING_WITH_LEN( " AWS " ) );
    signature->append( accKey );
    signature->append( 1, ':' );
    append64Encoded( signature, hash, hashSize );
}

//////////////////////////////////////////////////////////////////////////////
// Helper methods to deal with headers.

struct CurlListDeleter
{
    static void     free( curl_slist *curlList ){ dbgAssert( curlList ); curl_slist_free_all( curlList ); }
};

typedef auto_scope< curl_slist *, CurlListDeleter > ScopedCurlList;

static void
appendRequestHeader( const char *key, const char *value, ScopedCurlList *plist )
{
    dbgAssert( key );
    dbgAssert( plist );

    if( !value )
        return;

    std::string header;
    header.reserve( 128 );

    // Header is a "key: value" string.

    header.append( key );
    header.append( STRING_WITH_LEN( ": " ) );
    header.append( value );

    // Create a new list head and dupe the string.

    curl_slist *const newlist = curl_slist_append( *plist, header.c_str() );

    if( !newlist )
        throw std::bad_alloc();

    // Now the newlist is the new list.

    plist->release();
    *plist->unsafeAddress() = newlist;
}

static void
setRequestHeaders( const std::string &accKey, const std::string &secKey,
    const char *contentMd5, const char *contentType, unsigned cacheMaxAge, bool makePublic, bool srvEncrypt,
    const char *action, const char *bucketName, const char *key, WsStorType storType, 
    ScopedCurlList *plist )
{
    dbgAssert( plist );

    // Get current time.

    //$ FUTURE: have more elaborate logic here to get the time, as
    // the auth will fail if time is too skewed, so we should detect
    // time skewed errors and take the time value from the cloud.

    time_t local; 
    time ( &local );

    tm    gmtTime;
#ifdef _WIN32
    gmtime_s( &gmtTime, &local );
#else
    gmtime_r( &local, &gmtTime );
#endif

    char date[ 64 ];
    dbgVerify( strftime( date, sizeof( date ), 
        "%a, %d %b %Y %H:%M:%S GMT", 
        &gmtTime ) <  sizeof( date ) );

    // Calculate auth signature.

    std::string signature;

    calcSignature( accKey, secKey,
        contentMd5, contentType, date, makePublic, srvEncrypt,
        action, bucketName, key, storType,
        &signature );

    // Set request headers.

    // Header notes:
    //
    // Add empty Accept header otherwise curl will add Accept: */*
    //
    // We want to make sure that connection is kept alive between requests,
    // so set Keep-Alive explicitly.
    //
    // Note 1:
    //        Unfortunately this may cause a hang in old proxies that don't understand 
    //        Keep-Alive header and wait for the connection close.
    //        On the other hand, if this header is missing, AWS closes the connection.
    //        Let's choose perf and enable keep-alive. If we run into issues with 
    //        legacy proxies, we can make this parameter configurable.
    //
    // Note 2:
    //        Walrus keeps connection open for GET requests but closes for PUT requests
    //        (and potentially for others) ignoring "Connection: " header, 
    //        "Keep-Alive" has only a limited effect on Walrus connections.
    //
    // Note 3:
    //        If you capture traffic with Fiddler, you may see 2 headers "Connection: Keep-Alive".
    //        One of them is ours and the other is "Proxy-Connection: Keep-Alive" header 
    //        converted to "Connection:" by Fiddler.
    //        Without Fiddler, there should be exactly one "Connection: Keep-Alive".
    //        Having 2 with Fiddler seems fine as well.

    if( cacheMaxAge != WsConnection::c_noCacheControl )
    {
        std::string cacheControl( "public, max-age=" );
        char maxAge[ 16 ];
        uitoa( cacheMaxAge, maxAge );
        cacheControl += maxAge;
        appendRequestHeader( "Cache-Control", cacheControl.c_str(), plist );
    }

    appendRequestHeader( "Content-MD5", contentMd5, plist );
    appendRequestHeader( "Content-Type", contentType, plist );
    appendRequestHeader( "Date", date, plist );

    if( makePublic )
        appendRequestHeader( s_aclHeaderKey, s_aclHeaderValue, plist );

    if( srvEncrypt )
        appendRequestHeader( s_encryptHeaderKey, s_encryptHeaderValue, plist );

    appendRequestHeader( "Accept", "", plist );
    appendRequestHeader( "Authorization", signature.c_str(), plist );
    appendRequestHeader( "Connection", "Keep-Alive", plist );
    appendRequestHeader( "Expect", "", plist );
    appendRequestHeader( "Transfer-Encoding", "", plist );
}

//////////////////////////////////////////////////////////////////////////////
// Response loader to a given buffer.

struct WsGetResponseBufferLoader : public WsGetResponseLoader
{
                    WsGetResponseBufferLoader( void *buffer, size_t size );

    size_t          onLoad( const void *chunkData, size_t chunkSize, size_t totalSizeHint ) ;

    void           *p;
    size_t          left;
};

WsGetResponseBufferLoader::WsGetResponseBufferLoader( void *buffer, size_t size )
    : p( buffer )
    , left( size )
{
    dbgAssert( implies( size,  buffer ) );
}

size_t 
WsGetResponseBufferLoader::onLoad( const void *chunkData, size_t chunkSize, size_t totalSizeHint ) 
{
    if( !left )
    {
        return false;
    }

    size_t toCopy = std::min( chunkSize, left );
    memcpy( p, chunkData, toCopy );

    p = static_cast< unsigned char* >( p ) + toCopy;
    left -= toCopy;

    LOG_TRACE( "onLoad: loader=0x%llx, left=%llu, size=%llu", ( UInt64 )this, left, totalSizeHint );

    return toCopy;
}

//////////////////////////////////////////////////////////////////////////////
// Request uploader from a given buffer.

struct WsPutRequestBufferUploader : public WsPutRequestUploader
{
    WsPutRequestBufferUploader( const void *_buffer, size_t _size );

    void            setUpload( const void *_buffer, size_t _size );
    virtual size_t  onUpload( void *chunkBuf, size_t chunkSize );

    const void *    buffer;
    size_t          size;
    size_t          offset;
};

WsPutRequestBufferUploader::WsPutRequestBufferUploader( const void *_buffer, size_t _size )
    : buffer( _buffer ) 
    , size( _size )
    , offset( 0 )
{
}

void 
WsPutRequestBufferUploader::setUpload( const void *_buffer, size_t _size )
{
    dbgAssert( implies( size, buffer ) );
    buffer = _buffer;
    size = _size;
    offset = 0;
}

size_t  
WsPutRequestBufferUploader::onUpload( void *chunkBuf, size_t chunkSize )
{
    if( !size )
    {
        return 0;
    }

    dbgAssert( size >= offset );
    size_t toCopy = std::min( size - offset, chunkSize );

    memcpy( chunkBuf, static_cast< const char *>( buffer ) + offset, toCopy );
    offset += toCopy;

    LOG_TRACE( "onUpload: uploader=0x%llx, offset=%llu, size=%llu", ( UInt64 )this, offset, size );

    return toCopy;
}


//////////////////////////////////////////////////////////////////////////////
// Response handling.

enum WS_RESPONSE_STATUS
{
    WS_RESPONSE_STATUS_UNEXPECTED = -1,
    WS_RESPONSE_STATUS_SUCCESS,
    WS_RESPONSE_STATUS_FAILURE_WITH_DETAILS,
    WS_RESPONSE_STATUS_HTTP_FAILURE,
    WS_RESPONSE_STATUS_HTTP_RESOURSE_NOT_FOUND,
    WS_RESPONSE_STATUS_HTTP_OR_WS_FAILURE,
};

struct WsResponseDetails
{
                    WsResponseDetails();

    WS_RESPONSE_STATUS status;
    std::string     url;
    std::string     name;
                       
    // Common headers. 
                       
    std::string     httpStatus;
    std::string     httpDate;
    size_t          httpContentLength;
    std::string     httpContentType;
    std::string     amazonId;
    std::string     requestId;
    std::string     etag;

    // Common xml body elements.

    std::string     errorCode;
    std::string     errorMessage;
    std::string     hostId;
    bool            isTruncated;
    std::string     uploadId;


    // Length of the loaded content (in case of Get response)

    size_t          loadedContentLength;
};

WsResponseDetails::WsResponseDetails()
    : status( WS_RESPONSE_STATUS_UNEXPECTED )
    , httpContentLength( -1 ) 
    , isTruncated( false )
    , loadedContentLength( 0 )
{}

//////////////////////////////////////////////////////////////////////////////
// WebStor exception.

class WsException : public std::exception
{
public:
                    WsException( const char *fmt, ... );
    virtual         ~WsException() throw() {}
    virtual const char * what() const throw();

protected:
    std::vector< char > m_msg;
};

//////////////////////////////////////////////////////////////////////////////
// Base request handling.

class Request
{
public:
                    Request();
    virtual         ~Request();

    void            prepare( CURL *curl, char *errorBuffer, size_t errorBufferSize  );

    // Error support.

    void            saveError();
    void            raiseIfError();
    void            saveIfCurlError( CURLcode curlCode );
    void            saveBadAllocError() { m_is_bad_alloc = true; }
    bool            hasError() { return m_is_bad_alloc || m_error.get(); }
private:
                    Request( const Request& );
    Request&        operator=( const Request& );

protected:
    virtual void    onPrepare( CURL *curl )  { }

    // A reference to the curl handle and error buffer, Request doesn't own them!

    CURL           *m_curl;
    char           *m_curlErrorDetails;
    size_t          m_curlErrorDetailsSize;

    // Saved error. We need this to ensure 'nofail' behavior in callbacks that cannot throw
    // and to marshal the error between threads in async case.

    std::auto_ptr< WsException > m_error;
    bool            m_is_bad_alloc;
};

Request::Request()
    : m_curl( NULL )
    , m_is_bad_alloc( false )
{
}

Request::~Request()
{
}

void
Request::prepare( CURL *curl, char *errorBuffer, size_t errorBufferSize   )
{
    dbgAssert( curl );
    dbgAssert( !m_curl );
    dbgAssert( errorBuffer && errorBufferSize );

    m_curl = curl;
    m_curlErrorDetails = errorBuffer;
    m_curlErrorDetailsSize = errorBufferSize;

    // Clear the details.

    memset( m_curlErrorDetails, 0, m_curlErrorDetailsSize );

    onPrepare( curl );
}

void
Request::saveIfCurlError( CURLcode curlCode )
{
    if( curlCode == CURLE_OUT_OF_MEMORY )
    {
        saveBadAllocError();
        return;
    }

    // Ignore CURLE_WRITE_ERROR, when m_error is not set (see the check below), we should
    // treat CURLE_WRITE_ERROR as success. The error is returned only when our handleXXX methods 
    // processed a part of the response and don't care about the rest.

    if( curlCode != CURLE_OK && curlCode != CURLE_WRITE_ERROR )
    {
        // Check if we have any details about this error.

        dbgAssert( m_curlErrorDetails );

        if( *m_curlErrorDetails )
        {
            // Null terminate the errDetails.

            m_curlErrorDetails[ m_curlErrorDetailsSize - 1 ] = '\0';  // null terminate.
            m_error.reset( new WsException( errCurl, m_curlErrorDetails ) );
        }
        else
        {
            m_error.reset( new WsException( errCurl, curl_easy_strerror( curlCode ) ) );
        }

    }
}

void
Request::raiseIfError()
{
     if( m_is_bad_alloc )
     {
         throw std::bad_alloc();
     }

     if( m_error.get() )
     {
         throw *m_error;
     }
}

void
Request::saveError()
{
    // This function must be called from inside of a catch( ... ).

    try
    {
        try
        {
            throw;
        }
        catch( const std::bad_alloc & )
        {
            saveBadAllocError();
        }
        catch( const std::exception &e )
        {
            m_error.reset( new WsException( e.what() ) );
        }
        catch( ... )
        {
            m_error.reset( new WsException( errUnexpected ) );
        }
    }
    catch( const std::bad_alloc & )
    {
        saveBadAllocError();
    }
}

//////////////////////////////////////////////////////////////////////////////
// WebStor response Xml nodes.
// Values are sorted and order between enums and strings must match.

enum WS_RESPONSE_NODE
{
    WS_RESPONSE_NODE_BUCKET,
    WS_RESPONSE_NODE_CODE,
    WS_RESPONSE_NODE_COMMON_PREFIXES,
    WS_RESPONSE_NODE_CONTENTS,
    WS_RESPONSE_NODE_CREATION_DATE,
    WS_RESPONSE_NODE_ETAG,
    WS_RESPONSE_NODE_ERROR,
    WS_RESPONSE_NODE_HOST_ID,
    WS_RESPONSE_NODE_IS_TRUNCATED, 
    WS_RESPONSE_NODE_KEY,
    WS_RESPONSE_NODE_LAST_MODIFIED,
    WS_RESPONSE_NODE_MESSAGE,
    WS_RESPONSE_NODE_NAME,
    WS_RESPONSE_NODE_NEXT_MARKER,
    WS_RESPONSE_NODE_PREFIX,
    WS_RESPONSE_NODE_REQUEST_ID,
    WS_RESPONSE_NODE_SIZE,
    WS_RESPONSE_NODE_UPLOAD,
    WS_RESPONSE_NODE_UPLOAD_ID,
    WS_RESPONSE_NODE_LAST
};

static StringWithLen s_wsResponseNodeStrings[] =
{
    { STRING_WITH_LEN( "Bucket" ) },
    { STRING_WITH_LEN( "Code" ) },
    { STRING_WITH_LEN( "CommonPrefixes" ) },
    { STRING_WITH_LEN( "Contents" ) },
    { STRING_WITH_LEN( "CreationDate" ) },
    { STRING_WITH_LEN( "ETag" ) },
    { STRING_WITH_LEN( "Error" ) },
    { STRING_WITH_LEN( "HostId" ) },
    { STRING_WITH_LEN( "IsTruncated" ) },
    { STRING_WITH_LEN( "Key" ) },
    { STRING_WITH_LEN( "LastModified" ) },
    { STRING_WITH_LEN( "Message" ) },
    { STRING_WITH_LEN( "Name" ) },
    { STRING_WITH_LEN( "NextMarker" ) },
    { STRING_WITH_LEN( "Prefix" ) },
    { STRING_WITH_LEN( "RequestId" ) },
    { STRING_WITH_LEN( "Size" ) },
    { STRING_WITH_LEN( "Upload" ) },
    { STRING_WITH_LEN( "UploadId" ) },
};

CASSERT( dimensionOf( s_wsResponseNodeStrings ) == WS_RESPONSE_NODE_LAST );

//////////////////////////////////////////////////////////////////////////////
// WebStor operation request handling.

class WsRequest : public Request
{
public:
                    WsRequest( const char *name = NULL );
    virtual         ~WsRequest();

    // Sync execution.

    WsResponseDetails &execute(); 

    // Complete request.

    WsResponseDetails &complete( CURLcode curlCode );

    // Misc properties.

    const char*     url() { return m_responseDetails.url.c_str(); }
    void            setUrl( const char *url );

    const char *    name() { return m_responseDetails.name.c_str(); }

    const char *    httpVerb() { return onHttpVerb(); }

    ScopedCurlList  headers;
protected:
    
    // Override in derived class if you need to customize response payload parsing for
    // a specific WebStor operation.

    virtual size_t  onLoadBinary( const void *chunkData, size_t chunkSize, size_t totalSizeHint ) { return chunkSize; }
    virtual size_t  onUploadBinary( void *chunkBuf, size_t chunkSize ) { return 0; }
    virtual bool    onExpectXmlPayload( ) const { return false; }
    virtual bool    onStartXmlElement( ) { return true; }
    virtual bool    onEndXmlElement( ) {  return true; }
    virtual bool    onSetXmlValue( const char *value, int len ) { return true;  }

    virtual void    onPrepare( CURL *curl );
    virtual const char *onHttpVerb() = 0;

protected:
    void            throwXmlParserError() { throw WsException( errParser ); }

private:
    static size_t   handleHeader( const void *headerData, size_t count, size_t elementSize, void *ctx ); // nofail
    static size_t   handleXmlPayload( const void *chunkData, size_t count, size_t elementSize, void *ctx ); // nofail
    static size_t   handleBinaryLoad( const void *chunkData, size_t count, size_t elementSize, void *ctx ); // nofail
    static size_t   handleBinaryUpload( void *chunkBuf, size_t count, size_t elementSize, void *ctx ); // nofail

    void            setPayloadHandler();
    size_t          handleHeader_( const void *headerData, size_t count, size_t elementSize ); // nofail
    size_t          handleXmlPayload_( const void *chunkData, size_t count, size_t elementSize ); // nofail
    size_t          handleBinaryLoad_( const void *chunkData, size_t count, size_t elementSize ); // nofail
    size_t          handleBinaryUpload_( void *chunkBuf, size_t count, size_t elementSize ); // nofail

    static void     startXmlElement( void *ctx, const xmlChar *localName, 
                                    const xmlChar *prefix, const xmlChar *uri, 
                                    int cNamespaces, const xmlChar **pNamespaces,
                                    int cAttributes, int defaulted, const xmlChar **pAttributes );
    static void     setXmlValue( void *ctx, const xmlChar *value, int len );
    static void     endXmlElement( void *ctx, const xmlChar *localName, 
                                    const xmlChar *prefix, const xmlChar *uri );

    void            startXmlElementImpl( const xmlChar *localName );
    void            setXmlValueImpl( const xmlChar *value, int len );
    void            endXmlElementImpl( );

protected:

    // Xml parser and its state.

    xmlSAXHandler   m_parser;
    xmlParserCtxtPtr m_ctx;
    WS_RESPONSE_NODE m_stack[ 8 ];
    unsigned int    m_stackTop;

    // Request response.

    WsResponseDetails  m_responseDetails;
};

static bool 
strless( const StringWithLen &v1, const StringWithLen &v2 ) 
{ 
    return strcmp( v1.str, v2.str ) < 0; 
}

static WS_RESPONSE_NODE
getResponseNode( const char *nodeName )
{
    dbgAssert( nodeName );

#ifdef DEBUG
    {
        static bool s_dbgChecked = false;

        if( !s_dbgChecked )
        {
            for( size_t i = 1; i < WS_RESPONSE_NODE_LAST; ++i )
                dbgAssert( strcmp( s_wsResponseNodeStrings[ i-1 ].str, s_wsResponseNodeStrings[ i ].str ) < 0 );

            s_dbgChecked = true;
        }
    }
#endif  // DEBUG

    StringWithLen *first = &s_wsResponseNodeStrings[ 0 ];
    StringWithLen *last = &s_wsResponseNodeStrings[ WS_RESPONSE_NODE_LAST ];
    StringWithLen search = { nodeName, 0 /* not used */ };
    StringWithLen *it = std::lower_bound( first, last, search, strless );
    
    if( it == last || strcmp( nodeName, it->str ) )
    {
        // Not found.

        return WS_RESPONSE_NODE_LAST;
    }

    // Return the node index.

    return static_cast< WS_RESPONSE_NODE >( it - first );
};


static void 
composeUrl( const std::string &baseUrl, const char *bucketName, const char *key, 
    const char *keySuffix,
    std::string *url, std::string *escapedKey )
{
    dbgAssert( bucketName );
    dbgAssert( url );
    dbgAssert( implies( keySuffix, key ));
    dbgAssert( escapedKey );

    url->reserve( 512 );
    *url = baseUrl;
    url->append( bucketName );

    if( key )
    {
        url->append( 1, '/' );

        escapedKey->reserve( 64 );
        appendEscapedUrl( escapedKey, key );

        if( keySuffix )
        {
            escapedKey->append( keySuffix );
        }

        url->append( *escapedKey );
    }
}

static inline bool 
startsWith( const char *p, size_t size, const char *prefix, size_t prefixLen, 
    size_t *pprefixLen = NULL ) 
{
    if( pprefixLen )
    {
        *pprefixLen = prefixLen;
    }
    return size >= prefixLen && !strncmp( p, prefix, prefixLen );
}

WsRequest::WsRequest( const char *name )
    : m_responseDetails()
    , m_ctx( NULL )
    , m_stackTop( 0 )
{
    if( name )
    {
        m_responseDetails.name.assign( name );
    }

    memset ( &m_parser, 0, sizeof ( m_parser ) );
}

void
WsRequest::onPrepare( CURL *curl )
{
    curl_easy_setopt_checked( curl, CURLOPT_HEADERFUNCTION, handleHeader );
    curl_easy_setopt_checked( curl, CURLOPT_WRITEHEADER, this );

    curl_easy_setopt_checked( curl, CURLOPT_READFUNCTION, handleBinaryUpload );
    curl_easy_setopt_checked( curl, CURLOPT_READDATA, this );
}

void
WsRequest::setUrl( const char *url ) 
{ 
    dbgAssert( url ); 
    m_responseDetails.url.assign( url ); 
    curl_easy_setopt_checked( m_curl, CURLOPT_URL, m_responseDetails.url.c_str() );
}


WsRequest::~WsRequest()
{
    // Free the parser.

    xmlFreeParserCtxt ( m_ctx );  // it handles nulls.
}

WsResponseDetails &
WsRequest::execute()
{
    dbgAssert( !m_responseDetails.url.empty() ); 

    CURLcode curlCode = curl_easy_perform( m_curl ); 
    return complete( curlCode );
}

WsResponseDetails &
WsRequest::complete( CURLcode curlCode )
{
    saveIfCurlError( curlCode );

    if( m_ctx )
    {
        if( !hasError() )
        {
            // Complete parsing.

            xmlParseChunk( m_ctx, 0, 0, 1 );
        }

        xmlFreeParserCtxt( m_ctx );
        m_ctx = NULL;
    }

    raiseIfError();

    return m_responseDetails;
}

size_t
WsRequest::handleHeader( const void *headerData, size_t count, 
    size_t elementSize, void *ctx ) // nofail
{
    WsRequest *state = static_cast< WsRequest * >( ctx );
    return state->handleHeader_( headerData, count, elementSize ); // nofail
}

size_t
WsRequest::handleXmlPayload( const void *chunkData, size_t count,
    size_t elementSize, void *ctx ) // nofail
{
    WsRequest *state = static_cast< WsRequest * >( ctx );
    return state->handleXmlPayload_( chunkData, count, elementSize ); // nofail
}

size_t
WsRequest::handleBinaryLoad( const void *chunkData, size_t count, 
    size_t elementSize, void *ctx ) // nofail
{
    WsRequest *state = static_cast< WsRequest * >( ctx );
    return state->handleBinaryLoad_( chunkData, count, elementSize ); // nofail
}

size_t
WsRequest::handleBinaryUpload( void *chunkBuf, size_t count, 
    size_t elementSize, void *ctx ) // nofail
{
    WsRequest *state = static_cast< WsRequest * >( ctx );
    return state->handleBinaryUpload_( chunkBuf, count, elementSize ); // nofail
}

size_t
WsRequest::handleHeader_( const void *headerData, 
    size_t count, size_t memberSize ) // nofail
{
    try
    {
        const char *p = static_cast< const char * >( headerData );
        size_t size = count * memberSize;
        size_t originalSize = size;

        // Trim spaces and line-breaks from the start.

        while( size && ( *p == ' ' || *p == '\n' || *p == '\r' || *p == '\t' ) ) { ++p; --size; }

        if( !size )
        {
            return originalSize;
        } 

        const char *p1 = &p[ size - 1 ];
        while( size && ( *p1 == ' ' || *p1 == '\n' || *p1 == '\r' || *p1 == '\t' ) ) { --p1; --size; }

        if( !size )
        {
            return originalSize;
        } 

        // Parse the current header.

        size_t prefixLen = 0;

        if( startsWith( p, size, STRING_WITH_LEN( "HTTP" ), &prefixLen ) )
        {
            // Got HTTP response header.
            // Find and skip spaces.

            p += prefixLen;
            size -= prefixLen;
            while( size && *p != ' ' ) { ++p; --size; }
            while( size && *p == ' ' ) { ++p; --size; }

            m_responseDetails.httpStatus.assign( p, size );

            if( startsWith( p, size, STRING_WITH_LEN( "200 OK" ) ) || 
                startsWith( p, size, STRING_WITH_LEN( "204 No Content" ) ) ) 
            {
                m_responseDetails.status = WS_RESPONSE_STATUS_SUCCESS;
            }
            else if( startsWith( p, size, STRING_WITH_LEN( "404 Not" ) ) )
            {
                // Cloud storage services may return 404 if the resource is not found with xml
                // body containing more details about the error. So we may retrieve more details
                // about this case and this error may be promoted to
                // WS_RESPONSE_STATUS_FAILURE_WITH_DETAILS later.

                m_responseDetails.status = WS_RESPONSE_STATUS_HTTP_RESOURSE_NOT_FOUND;
            } 
            else if( startsWith( p, size, STRING_WITH_LEN( "301 Moved" ) ) ||
                startsWith( p, size, STRING_WITH_LEN( "400 Bad" ) ) ||
                startsWith( p, size, STRING_WITH_LEN( "403 Forbidden" ) ) ||
                startsWith( p, size, STRING_WITH_LEN( "409 Conflict" ) ) ||
                startsWith( p, size, STRING_WITH_LEN( "500 Internal" ) ) ||
                startsWith( p, size, STRING_WITH_LEN( "503 Service" ) ) ||
                startsWith( p, size, STRING_WITH_LEN( "503 Slow" ) ) )
            {
                // Try to read detailed error info from the payload.
                // This error may be promoted to
                // WS_RESPONSE_STATUS_FAILURE_WITH_DETAILS later.

                m_responseDetails.status = WS_RESPONSE_STATUS_HTTP_OR_WS_FAILURE;
            } 
            else
            {
                m_responseDetails.status = WS_RESPONSE_STATUS_HTTP_FAILURE;
            }

            // Set up the corresponding body loader.

            setPayloadHandler();
        }
        else if( startsWith( p, size, STRING_WITH_LEN( "ETag: \""), &prefixLen ) )
        {
            // Amazon returns "ETag" header with quotes.

            size -= prefixLen;
            size -= ( size > 0 ); // trim the last '"'
            m_responseDetails.etag.assign( p + prefixLen, size ); 
        }
        else if( startsWith( p, size, STRING_WITH_LEN( "ETag: "), &prefixLen ) )
        {
            // Walrus returns "ETag" header without quotes.

            m_responseDetails.etag.assign( p + prefixLen, size - prefixLen ); 
        }
        else if( startsWith( p, size, STRING_WITH_LEN( "Date: " ), &prefixLen ) )
        {
            m_responseDetails.httpDate.assign( p + prefixLen, size - prefixLen ); 
        }
        else if( startsWith( p, size, STRING_WITH_LEN( "x-amz-id-2: " ), &prefixLen ) )
        {
            m_responseDetails.amazonId.assign( p + prefixLen, size - prefixLen ); 
        }
        else if( startsWith( p, size, STRING_WITH_LEN( "x-amz-request-id: " ), &prefixLen ) )
        {
            m_responseDetails.requestId.assign( p + prefixLen, size - prefixLen );
        }
        else if( startsWith( p, size, STRING_WITH_LEN( "Content-Length: " ), &prefixLen ) )
        {
            // Null terminate.

            std::string tmp( p + prefixLen, size - prefixLen );
            m_responseDetails.httpContentLength = atoll( tmp.c_str() );

            // Set up the corresponding body loader.

            setPayloadHandler();
        }
        else if( startsWith( p, size, STRING_WITH_LEN( "Content-Type: " ), &prefixLen ) )    
        {
            m_responseDetails.httpContentType.assign( p + prefixLen, size - prefixLen );

            // Set up the corresponding body loader.

            setPayloadHandler();
        } 

        // Return actual number of bytes read (if we return less, curl will 
        // treat it as an error).

        return originalSize;
    }

    catch( ... )
    {
        saveError();
    }

    //  Stop the farther processing, let curl return CURLE_WRITE_ERROR.

    return 0;
}


size_t
WsRequest::handleXmlPayload_( const void *chunkData, size_t count, size_t memberSize ) // nofail
{
    try
    {
        if( (!m_ctx) && 
            ( m_responseDetails.status == WS_RESPONSE_STATUS_SUCCESS ||
            m_responseDetails.status == WS_RESPONSE_STATUS_HTTP_RESOURSE_NOT_FOUND  ||
            m_responseDetails.status == WS_RESPONSE_STATUS_HTTP_OR_WS_FAILURE) ) 
        {
            // Create the SAX parser.

            m_parser.initialized = XML_SAX2_MAGIC;
            m_parser.startElementNs = &startXmlElement;
            m_parser.characters     = &setXmlValue;
            m_parser.endElementNs   = &endXmlElement;

            m_ctx = xmlCreatePushParserCtxt ( &m_parser, this, NULL, 0, 0 );

            if( !m_ctx )
            {
                throw std::bad_alloc(); 
            }
        }

        // Parse the current chunk.
        size_t size = count * memberSize;
        xmlParseChunk( m_ctx, static_cast< const char * >( chunkData ), size, 0 );

        return size;
    }

    catch( ... )
    {
        saveError();
    }

    // Stop the farther processing, let curl return CURLE_WRITE_ERROR.

    return 0;
}

size_t
WsRequest::handleBinaryLoad_( const void *chunkData, size_t count, 
    size_t elementSize ) // nofail
{
    size_t loaded = 0;
    size_t chunkSize = count * elementSize;

    try
    {
        loaded = onLoadBinary( chunkData, chunkSize, 
            m_responseDetails.httpContentLength == -1 ? 0 : m_responseDetails.httpContentLength /* hint */ );

        dbgAssert( loaded <= chunkSize );
        m_responseDetails.loadedContentLength += loaded;
    }
    catch( ... )
    {
        saveError();
    }

    if( loaded < chunkSize )
    {
        m_responseDetails.isTruncated = true;
    }

    //  If loaded < chunkSize, curl will cancel response handling and return CURLE_WRITE_ERROR.

    return loaded;
}

size_t
WsRequest::handleBinaryUpload_( void *chunkBuf, size_t count, 
    size_t elementSize ) // nofail
{
    size_t uploaded = 0;
    size_t chunkSize = count * elementSize;

    try
    {
        uploaded = onUploadBinary( chunkBuf, chunkSize );
    }
    catch( ... )
    {
        saveError();
    }

    return uploaded;
}

void
WsRequest::startXmlElement( void *ctx, const xmlChar *localName, 
    const xmlChar *prefix, const xmlChar *uri, 
    int cNamespaces, const xmlChar **pNamespaces,
    int cAttributes, int defaulted, const xmlChar **pAttributes ) 
{
    WsRequest *state = static_cast< WsRequest * > ( ctx );
    state->startXmlElementImpl( localName );
}

void
WsRequest::setXmlValue( void *ctx, const xmlChar *value, int len ) 
{
    WsRequest *state = static_cast< WsRequest * > ( ctx );
    state->setXmlValueImpl( value, len );
}

void
WsRequest::endXmlElement( void *ctx, const xmlChar *localName, 
    const xmlChar *prefix, const xmlChar *uri )
{
    WsRequest *state = static_cast< WsRequest * > ( ctx );
    state->endXmlElementImpl();
}

void
WsRequest::startXmlElementImpl( const xmlChar *localName ) 
{
    if( m_stackTop >= dimensionOf( m_stack ) )
    {
        throwXmlParserError();
    }

    WS_RESPONSE_NODE node = getResponseNode( reinterpret_cast< const char* >( localName ) );
    m_stack[ m_stackTop++ ] = node;

    if( !onStartXmlElement() )
    {
        throwXmlParserError();
    }
}

void
WsRequest::setXmlValueImpl( const xmlChar *value, int len ) 
{
    // Handle common errors.

    const char * p = reinterpret_cast< const char * >( value );

    if( m_stackTop == 2 && m_stack[ 0 ] == WS_RESPONSE_NODE_ERROR )
    {
        switch( m_stack[ 1 ] )
        {
        case WS_RESPONSE_NODE_CODE:
            m_responseDetails.errorCode.assign( p, len );
            break;
        case WS_RESPONSE_NODE_MESSAGE:
            m_responseDetails.errorMessage.assign( p, len );
            break;
        case WS_RESPONSE_NODE_REQUEST_ID:
            m_responseDetails.requestId.assign( p, len );
            break;
        case WS_RESPONSE_NODE_HOST_ID:
            m_responseDetails.hostId.assign( p, len );
            break;
        default:
            break;
        }

        if( m_responseDetails.status == WS_RESPONSE_STATUS_HTTP_RESOURSE_NOT_FOUND ||
            m_responseDetails.status == WS_RESPONSE_STATUS_HTTP_OR_WS_FAILURE )
        {
            // Promote to more detailed error.

            m_responseDetails.status = WS_RESPONSE_STATUS_FAILURE_WITH_DETAILS;
        }
    }

    if( !onSetXmlValue( p, len ) )
    {
        throwXmlParserError( );
    }
}

void
WsRequest::endXmlElementImpl( )
{
    if( !m_stackTop || !onEndXmlElement() )
    {
        throwXmlParserError();
        return;
    }

    m_stackTop--;
}

void
WsRequest::setPayloadHandler()
{
    if( m_responseDetails.status == WS_RESPONSE_STATUS_SUCCESS )
    {
        // Some requests expect to get structured data in XML format;
        // others get raw data.

        curl_easy_setopt_checked( m_curl, CURLOPT_WRITEFUNCTION, onExpectXmlPayload() ? handleXmlPayload : handleBinaryLoad );
        curl_easy_setopt_checked( m_curl, CURLOPT_WRITEDATA, this );
    }
    else if( m_responseDetails.httpContentLength != 0 &&
        strstr( m_responseDetails.httpContentType.c_str(), s_contentTypeXml ) )
    {
        // Error conditions may have details in XML.

        curl_easy_setopt_checked( m_curl, CURLOPT_WRITEFUNCTION, handleXmlPayload );
        curl_easy_setopt_checked( m_curl, CURLOPT_WRITEDATA, this );
    }
}

//////////////////////////////////////////////////////////////////////////////
// Response handling for 'get' operation.

class WsGetRequest : public WsRequest
{
public:
                    WsGetRequest( const char *name, WsGetResponseLoader *loader );
                    WsGetRequest( const char *name, void *buffer, size_t size );

private:
    virtual size_t  onLoadBinary( const void *chunkData, size_t chunkSize, size_t totalSizeHint );
    virtual void    onPrepare( CURL *curl );
    virtual const char *onHttpVerb() { return "GET"; }

    WsGetResponseBufferLoader m_builtinLoader;
    WsGetResponseLoader *m_loader;
};

WsGetRequest::WsGetRequest( const char *name, WsGetResponseLoader *loader )
    : WsRequest( name )
    , m_builtinLoader( NULL, 0 )
    , m_loader( loader )
{
}

WsGetRequest::WsGetRequest( const char *name, void *buffer, size_t size )
    : WsRequest( name )
    , m_builtinLoader( buffer, size )
    , m_loader( &m_builtinLoader )
{
}

size_t  
WsGetRequest::onLoadBinary( const void *chunkData, size_t chunkSize, size_t totalSizeHint )
{
    return m_loader->onLoad( chunkData, chunkSize, totalSizeHint );
}

void 
WsGetRequest::onPrepare( CURL *curl )
{
    WsRequest::onPrepare( curl );
    curl_easy_setopt_checked( curl, CURLOPT_HTTPGET, 1 );
}

//////////////////////////////////////////////////////////////////////////////
// Response handling for 'put' operation.

class WsPutRequest : public WsRequest
{
public:
                    WsPutRequest( const char *name, WsPutRequestUploader *uploader,
                        size_t totalSize );
                    WsPutRequest( const char *name, const void *data = NULL, size_t size = 0 );

    void            setUpload( const void *data, size_t size );

private:
    virtual size_t  onUploadBinary( void *chunkBuf, size_t chunkSize );
    virtual void    onPrepare( CURL *curl );
    virtual const char *onHttpVerb() { return "PUT"; }

    WsPutRequestBufferUploader m_builtinUploader;
    WsPutRequestUploader *m_uploader;
    size_t          m_totalSize;
};


WsPutRequest::WsPutRequest( const char *name, WsPutRequestUploader *uploader,
                            size_t totalSize )
    : WsRequest( name )
    , m_builtinUploader( NULL, 0 )
    , m_uploader( uploader )
    , m_totalSize( totalSize )
{
}

WsPutRequest::WsPutRequest( const char *name, const void *data, size_t size )
    : WsRequest( name )
    , m_builtinUploader( data, size )
    , m_uploader( &m_builtinUploader )
    , m_totalSize( size )
{
}

size_t
WsPutRequest::onUploadBinary( void *chunkBuf, size_t chunkSize )
{
     return m_uploader->onUpload( chunkBuf, chunkSize );
}

void
WsPutRequest::setUpload( const void *data, size_t size )
{
    dbgAssert( m_curl );
    dbgAssert( &m_builtinUploader == m_uploader );

    m_builtinUploader.setUpload( data, size );
    m_totalSize = size;
    curl_easy_setopt_checked( m_curl, CURLOPT_INFILESIZE, size );
}

void
WsPutRequest::onPrepare( CURL *curl )
{
    WsRequest::onPrepare( curl );
    curl_easy_setopt_checked( curl, CURLOPT_INFILESIZE, m_totalSize );
    curl_easy_setopt_checked( curl, CURLOPT_UPLOAD, 1 );
}

//////////////////////////////////////////////////////////////////////////////
// Response handling for 'del' operation.

class WsDelRequest: public WsRequest
{
public:
                    WsDelRequest( const char *name = NULL );
private:
    virtual void    onPrepare( CURL *curl );
    virtual const char *onHttpVerb() { return "DELETE"; }
};

WsDelRequest::WsDelRequest( const char *name )
    : WsRequest( name )
{
}

void
WsDelRequest::onPrepare( CURL *curl )
{
    WsRequest::onPrepare( curl );
    curl_easy_setopt_checked( m_curl, CURLOPT_CUSTOMREQUEST, httpVerb() );
}

//////////////////////////////////////////////////////////////////////////////
// Response handling for 'listObjects' operation.

class WsListBucketsRequest : public WsRequest
{
public:
                    WsListBucketsRequest( std::vector< WsBucket > *buckets );

private:
    virtual bool    onExpectXmlPayload() const { return true; }
    virtual bool    onStartXmlElement();
    virtual bool    onEndXmlElement();
    virtual bool    onSetXmlValue( const char *value, int len );
   
    virtual void    onPrepare( CURL *curl );
    virtual const char *onHttpVerb() { return "GET"; }

private:
    bool            isBucketNode();
    
    WsBucket        m_current;
    std::vector< WsBucket > *m_buckets;
};


WsListBucketsRequest::WsListBucketsRequest( std::vector< WsBucket > *buckets )
    : WsRequest()
    , m_buckets( buckets )
{
    dbgAssert( buckets );
}

bool    
WsListBucketsRequest::onStartXmlElement() 
{ 
    if( isBucketNode() )
    {
        m_current.clear();
    }

    return true;
}

bool
WsListBucketsRequest::onEndXmlElement() 
{ 
    if( isBucketNode() )
    {
        m_buckets->push_back( m_current );
    }

    return true;
}

bool
WsListBucketsRequest::onSetXmlValue( const char *value, int len ) 
{
    if( m_stackTop < 3 )
    {
        return true;
    }

    switch( m_stack[ m_stackTop - 1 ] )
    {
    case WS_RESPONSE_NODE_NAME:
        m_current.name.assign( value, len );
        break;

    case WS_RESPONSE_NODE_CREATION_DATE:
        m_current.creationDate.assign( value, len );
        break;
    default:
        break;
    }

    return true;
}

bool
WsListBucketsRequest::isBucketNode()
{
    return ( m_stackTop == 3 || m_stackTop == 4 ) && 
        ( m_stack[ m_stackTop - 1 ] == WS_RESPONSE_NODE_BUCKET );
}

void 
WsListBucketsRequest::onPrepare( CURL *curl )
{
    WsRequest::onPrepare( curl );
    curl_easy_setopt_checked( curl, CURLOPT_HTTPGET, 1 );
}


//////////////////////////////////////////////////////////////////////////////
// Response handling for 'listObjects' operation.

class WsListObjectsRequest : public WsRequest
{
public:
                    WsListObjectsRequest( const char *prefix, 
                        WsObjectEnum *objectEnum, WsStorType storType );

    const std::string & nextMarker() const { return m_nextMarker.empty() ? m_current.key : m_nextMarker; }
private:
    virtual bool    onExpectXmlPayload() const { return true; }
    virtual bool    onStartXmlElement( ) ;
    virtual bool    onEndXmlElement( ) ;
    virtual bool    onSetXmlValue( const char *value, int len ) ;
   
    virtual void    onPrepare( CURL *curl );
    virtual const char *onHttpVerb() { return "GET"; }

    bool            isObjectNode();
    
    WsObject        m_current;
    WsObjectEnum   *m_objectEnum;
    WsStorType      m_storType;
    std::string     m_prefix;
    std::string     m_nextMarker;
};


WsListObjectsRequest::WsListObjectsRequest( const char *prefix, 
    WsObjectEnum *objectEnum, WsStorType storType )
    : WsRequest( prefix )
    , m_objectEnum( objectEnum )
    , m_storType( storType )
{
    dbgAssert( objectEnum );
}

bool
WsListObjectsRequest::onStartXmlElement() 
{ 
    if( isObjectNode() )
    {
        m_current.clear();
    }

    return true;
}

bool
WsListObjectsRequest::onEndXmlElement() 
{ 
    if( isObjectNode() )
    {
        return m_objectEnum->onObject( m_current );
    }

    return true;
}

bool
WsListObjectsRequest::onSetXmlValue( const char *value, int len ) 
{
    if( m_stackTop < 2 )
    {
        return true;
    }

    switch( m_stack[ m_stackTop - 1 ] )
    {
    case WS_RESPONSE_NODE_IS_TRUNCATED:
        m_responseDetails.isTruncated = ( len == 4 && !strncmp( value, "true", 4 ) );
        break;

    case WS_RESPONSE_NODE_KEY:

        // Append value here instead of assign because it can be passed in chunks.

        m_current.key.append( value, len );
        break;

    case WS_RESPONSE_NODE_LAST_MODIFIED:
        m_current.lastModified.assign( value, len );
        break;

    case WS_RESPONSE_NODE_ETAG:

        // Skip beginning and trailing quotes.

        if( len && *value == '"' )
            ++value, --len;

        if( len && value[ len - 1 ] == '"' )
            --len;

        m_current.etag.append( value, len );
        break;

    case WS_RESPONSE_NODE_SIZE:
        {
            std::string tmp( value, len );
            m_current.size = atoll( tmp.c_str() );
            break;
        }

    case WS_RESPONSE_NODE_PREFIX:
        if( m_stack[ m_stackTop - 2 ] == WS_RESPONSE_NODE_COMMON_PREFIXES )
        {
            if( m_storType == WST_WALRUS )
            {
                m_current.key.append( m_prefix );
            }
            m_current.key.append( value, len );
            m_current.isDir = true;
        }
        else if( m_storType == WST_WALRUS )
        {
            m_prefix.assign( value, len );
        }
        break;

    case WS_RESPONSE_NODE_NEXT_MARKER:
        m_nextMarker.assign( value, len );
        break;

    default:
        break;
    }

    return true;
}

bool
WsListObjectsRequest::isObjectNode()
{
    if( m_storType != WST_WALRUS )
    {
        return m_stackTop == 2 && 
            ( m_stack[ m_stackTop - 1 ] == WS_RESPONSE_NODE_CONTENTS || 
            m_stack[ m_stackTop - 1 ] == WS_RESPONSE_NODE_COMMON_PREFIXES );
    }
    else
    {
      return (m_stackTop == 3 && m_stack[ m_stackTop - 1 ] == WS_RESPONSE_NODE_CONTENTS) ||
          (m_stackTop == 4 && 
          m_stack[ m_stackTop - 1 ] == WS_RESPONSE_NODE_PREFIX && 
          m_stack[ m_stackTop - 2 ] == WS_RESPONSE_NODE_COMMON_PREFIXES);
    }
}

void
WsListObjectsRequest::onPrepare( CURL *curl )
{
    WsRequest::onPrepare( curl );
    curl_easy_setopt_checked( m_curl, CURLOPT_HTTPGET, 1 );
}

//////////////////////////////////////////////////////////////////////////////
// Response handling for 'initiateMultipartUpload' operation.

class WsInitiateMultipartUploadRequest : public WsRequest
{
public:
                    WsInitiateMultipartUploadRequest( const char *name );

private:
    virtual bool    onExpectXmlPayload() const { return true; }
    virtual bool    onSetXmlValue( const char *value, int len ) ;

    virtual void    onPrepare( CURL *curl );
    virtual const char *onHttpVerb() { return "POST"; }
};


WsInitiateMultipartUploadRequest::WsInitiateMultipartUploadRequest( const char *name )
    : WsRequest( name )
{
}

bool
WsInitiateMultipartUploadRequest::onSetXmlValue( const char *value, int len ) 
{
    if( m_stackTop == 2 && m_stack[ m_stackTop - 1 ] == WS_RESPONSE_NODE_UPLOAD_ID )
    {
        m_responseDetails.uploadId.assign( value, len );
    }

    return true;
}

void
WsInitiateMultipartUploadRequest::onPrepare( CURL *curl )
{
    WsRequest::onPrepare( curl );
    curl_easy_setopt_checked( m_curl, CURLOPT_POST, 1 );
    curl_easy_setopt_checked( m_curl, CURLOPT_POSTFIELDSIZE, 0 );
}

//////////////////////////////////////////////////////////////////////////////
// Response handling for 'completeMultipartUpload' operation.

class WsCompleteMultipartUploadRequest : public WsRequest
{
public:
                    WsCompleteMultipartUploadRequest( const char *name );

    void            setUpload( const void *data, size_t size );

private:
    virtual bool    onExpectXmlPayload() const { return true; }
    virtual bool    onSetXmlValue( const char *value, int len ) ;

    virtual size_t  onUploadBinary( void *chunkBuf, size_t chunkSize );
    virtual void    onPrepare( CURL *curl );
    virtual const char *onHttpVerb() { return "POST"; }

    WsPutRequestBufferUploader m_builtinUploader;
};

WsCompleteMultipartUploadRequest::WsCompleteMultipartUploadRequest( const char *name )
    : WsRequest( name )
    , m_builtinUploader( NULL, 0 )
{
}

bool
WsCompleteMultipartUploadRequest::onSetXmlValue( const char *value, int len )
{
    if( m_stackTop == 2 && m_stack[ m_stackTop - 1 ] == WS_RESPONSE_NODE_ETAG )
    {
        // Skip beginning and trailing quotes.

        if( len != 1 || *value != '"' )
        {
            m_responseDetails.etag.append( value, len );
        }
    }

    return true;
}

size_t   
WsCompleteMultipartUploadRequest::onUploadBinary( void *chunkBuf, size_t chunkSize )
{
     return m_builtinUploader.onUpload( chunkBuf, chunkSize );
}

void  
WsCompleteMultipartUploadRequest::setUpload( const void *data, size_t size )
{
    dbgAssert( m_curl );
    m_builtinUploader.setUpload( data, size );
    curl_easy_setopt_checked( m_curl, CURLOPT_POSTFIELDSIZE, size );
}

void    
WsCompleteMultipartUploadRequest::onPrepare( CURL *curl )
{
    WsRequest::onPrepare( curl );
    curl_easy_setopt_checked( m_curl, CURLOPT_POST, 1 );
}

//////////////////////////////////////////////////////////////////////////////
// Response handling for 'listMultipartUpload' operation.

class WsListMultipartUploadsRequest : public WsRequest
{
public:
                    WsListMultipartUploadsRequest( const char *name,
                            WsMultipartUploadEnum *uploadEnum );
        
    const WsMultipartUpload &lastUpload() const { return m_current; }

protected:
    virtual bool    onExpectXmlPayload() const { return true; }
    virtual bool    onStartXmlElement( ) ;
    virtual bool    onEndXmlElement( ) ;
    virtual bool    onSetXmlValue( const char *value, int len ) ;
   
    virtual void    onPrepare( CURL *curl );
    virtual const char *onHttpVerb() { return "GET"; }

private:
    bool            isUploadNode();

    WsMultipartUpload       m_current;
    WsMultipartUploadEnum * m_uploadEnum;
};

WsListMultipartUploadsRequest::WsListMultipartUploadsRequest( const char *name, 
    WsMultipartUploadEnum *uploadEnum )
    : WsRequest( name )
    , m_uploadEnum( uploadEnum )
{
    dbgAssert( uploadEnum );
}

bool
WsListMultipartUploadsRequest::onStartXmlElement( ) 
{ 
    if( isUploadNode() )
    {
        m_current.clear();
    }

    return true;
}

bool
WsListMultipartUploadsRequest::onEndXmlElement( ) 
{ 
    if( isUploadNode() )
    {
        return m_uploadEnum->onUpload( m_current );
    }

    return true;
}

bool
WsListMultipartUploadsRequest::onSetXmlValue( const char *value, int len ) 
{
    if( m_stackTop < 2 )
    {
        return true;
    }

    switch( m_stack[ m_stackTop - 1 ] )
    {
    case WS_RESPONSE_NODE_IS_TRUNCATED:
        m_responseDetails.isTruncated = ( len == 4 && !strncmp( value, "true", 4 ) );
        break;

    case WS_RESPONSE_NODE_KEY:

        // Append value here instead of assign because it can be passed in chunks.

        m_current.key.append( value, len );
        break;

    case WS_RESPONSE_NODE_UPLOAD_ID:
        m_current.uploadId.assign( value, len );
        break;

    case WS_RESPONSE_NODE_PREFIX:
        if( m_stack[ m_stackTop - 2 ] == WS_RESPONSE_NODE_COMMON_PREFIXES )
        {
            m_current.key.append( value, len );
            m_current.isDir = true;
        }
        break;
    default:
        break;
    }

    return true;
}

inline bool
WsListMultipartUploadsRequest::isUploadNode()
{
    return m_stackTop == 2 && 
        ( m_stack[ m_stackTop - 1 ] == WS_RESPONSE_NODE_UPLOAD || 
        m_stack[ m_stackTop - 1 ] == WS_RESPONSE_NODE_COMMON_PREFIXES );
}

void
WsListMultipartUploadsRequest::onPrepare( CURL *curl )
{
    WsRequest::onPrepare( curl );
    curl_easy_setopt_checked( curl, CURLOPT_HTTPGET, 1 );
}

//////////////////////////////////////////////////////////////////////////////
// General error handing.

static void 
handleErrors( const WsResponseDetails &details )
{
    switch( details.status ) 
    {
    case WS_RESPONSE_STATUS_SUCCESS:
        break;

    case WS_RESPONSE_STATUS_UNEXPECTED:
        {
            // HTTP header is missing in the response, this should never happen.

            throw WsException( errUnexpected );
        }

    case WS_RESPONSE_STATUS_HTTP_RESOURSE_NOT_FOUND:
        {
            throw WsException( errHTTPResourceNotFound, details.url.c_str() );
        }

    case WS_RESPONSE_STATUS_HTTP_FAILURE:
    case WS_RESPONSE_STATUS_HTTP_OR_WS_FAILURE: // could not read more details from the payload, treat as http error.
        {
            throw WsException( errHTTP, details.httpStatus.c_str() );
        }

    case WS_RESPONSE_STATUS_FAILURE_WITH_DETAILS:
        {
            //$ REVIEW: add details.amazonId and details.hostId
            // to the exception class (without forcing them into
            // the message).

            throw WsException( errWsDetail, 
                details.errorMessage.c_str(), 
                details.errorCode.c_str(), 
                details.requestId.c_str() );
        }

    default:
        dbgPanicSz( "BUG: unknown response status!!!" );
    }
}

static void
throwSummary( const char *op, const char *key )
{
    // This function must be called from inside of a catch( ... ).

    try
    {
        throw;
    }
    catch( const std::bad_alloc & )
    {
        throw;
    }
    catch( const std::exception &e )
    {
        throw WsException( errOpSummary, op, key, e.what() );
    }
    catch( ... )
    {
        throw WsException( errOpSummary, op, key, errUnexpected );
    }
}

//////////////////////////////////////////////////////////////////////////////
// WsConnection.

WsConnection::WsConnection( const WsConfig &config )
    : m_accKey( config.accKey )
    , m_secKey( config.secKey )
    , m_baseUrl()
    , m_proxy( config.proxy ? config.proxy : "" )
    , m_storType( config.storType )
    , m_isHttps( config.isHttps )
    , m_sslCertFile( config.sslCertFile ? config.sslCertFile : "" )
    , m_traceCallback( NULL )
    , m_asyncRequest( NULL )
    , m_timeout( s_defaultTimeout )      
    , m_connectTimeout( s_defaultConnectTimeout )
{
    CASSERT( dimensionOf( m_errorBuffer ) >= CURL_ERROR_SIZE );

    memset( m_errorBuffer, 0, sizeof( m_errorBuffer ) );

    // Construct a base url for all subsequent requests.

    m_baseUrl = config.isHttps ? "https://" : "http://";

    m_baseUrl.append( config.host && *config.host ? config.host : ( config.storType == WST_GCS ? s_defaultGcsHost : s_defaultS3Host ) );

    const char* port = config.port;

    if( config.storType == WST_WALRUS && ( !port || !*port ) )
    {
        port = s_defaultWalrusPort;
    }

    if( port && *port )
    {
        m_baseUrl.append( 1, ':' );
        m_baseUrl.append( port );
    }

    if( config.storType == WST_WALRUS )
    {
        m_baseUrl.append( STRING_WITH_LEN( "/services/Walrus" ) );
    }

    m_baseUrl.append( 1, '/' );

    // Extract region from the host name:
    // s3-us-west-2.amazonaws.com => us-west-2
    // s3.amazonaws.com => <empty>

    StringWithLen prefix = { STRING_WITH_LEN( "s3-" ) };

    if ( config.storType == WST_S3 && config.host && !strncmp( config.host, prefix.str, prefix.len ) )
    {
        dbgAssert( !strncmp( s_defaultS3Host, STRING_WITH_LEN( "s3." ) ) );

        const char *p = config.host + prefix.len;
        const char *p2 = strstr( p, s_defaultS3Host + 2 );

        if( p2 )
        {
            m_region.assign( p, p2 - p );
        }
    }
}

WsConnection::~WsConnection()
{
    cancelAsync();  // nofail
}

void
WsConnection::cancelAsync()  // nofail
{
    std::auto_ptr< Request > request( m_asyncRequest );
    m_asyncRequest = NULL;

    m_curl.cancelOp();  // nofail
}

bool
WsConnection::isAsyncPending()
{
    return !!m_asyncRequest;
}

bool
WsConnection::isAsyncCompleted() 
{
    return m_asyncRequest && m_curl.isOpCompleted();
}

int       
WsConnection::waitAny( WsConnection **cons, size_t count, size_t startFrom, long timeout )
{
    CASSERT( c_maxWaitAny == EventSync::c_maxEventCount );
    dbgAssert( implies( count, cons ) );

    if( count > EventSync::c_maxEventCount )
    {
        throw WsException( errTooManyConnetions );
    }

    EventSync *events[ EventSync::c_maxEventCount ] = {};

    for( size_t i = 0; i < count; ++i )
    {
        // Get the connection starting with 'startFrom'
        // to ensure fairness.

        size_t index = ( i + startFrom ) % count;
        WsConnection &con = *( cons[ index ] );

        dbgAssert( con.isAsyncPending() );

        if( con.isAsyncCompleted() )
        {
            return index;
        }

        // Remember in the array starting from 0.

        events[ i ] = con.m_curl.completedEvent();
    }

    int res = EventSync::waitAny( events, count, static_cast< UInt32 >( timeout ) );

    if( res == -1 )
    {
        return -1; // timeout
    }
    
    // res is "0-based", get the connection index.

    return ( res + startFrom ) % count;
}

static curl_socket_t 
onSocketOpen( void *, curlsocktype, curl_sockaddr *addr )
{
    dbgAssert( addr );

    // Open socket.

    curl_socket_t sockfd = socket( addr->family, addr->socktype, addr->protocol );

    // Enable TCP keep alive.

    setTcpKeepAlive( ( SocketHandle )sockfd, &s_tcpKeepAliveProbes );

    // Increase send and receive buffers.
   
    setSocketBuffers( ( SocketHandle )sockfd, s_socketBufferSize );

    return sockfd;
}

static size_t   
writeNoop( const void *chunkData, size_t count, size_t elementSize, void *ctx ) // nofail
{ 
    return count * elementSize; 
}

void
WsConnection::prepare( WsRequest *request, const char *bucketName, const char *key,
        const char *contentType, unsigned cacheMaxAge, bool makePublic, bool useSrvEncrypt )
{
    dbgAssert( !m_asyncRequest ); // some async operation is in progress, need to complete/cancel it first 
                                  // before starting a new one.
    dbgAssert( request );

    // We reuse connections, so reset the connection first
    // to make sure it's clean from the previous request.
    // Reset() preserves live connections, the Session ID cache, the DNS cache, the cookies 
    // and doesn't require a new SSL handshake.
    
    curl_easy_reset( m_curl );

    // Set TCP KeepAlive.

    curl_easy_setopt_checked( m_curl, CURLOPT_OPENSOCKETFUNCTION, onSocketOpen );

    // Set re-use connection.

    curl_easy_setopt_checked( m_curl, CURLOPT_FRESH_CONNECT, 0 );

    // Set error buffer (it should be available through m_curl lifetime).

    curl_easy_setopt_checked( m_curl, CURLOPT_ERRORBUFFER, m_errorBuffer );

    // Set timeouts.

    curl_easy_setopt_checked( m_curl, CURLOPT_TIMEOUT_MS, m_timeout );
    curl_easy_setopt_checked( m_curl, CURLOPT_CONNECTTIMEOUT_MS, m_connectTimeout );

    // Disable signal usage by libcurl. 
    // Libcurl uses signals to interrupt slow dns resolver by calling alarm(),
    // unfortunately that logic is not reliable and causes "longjmp causes uninitialized stack frame",
    // see "http://curl.haxx.se/mail/lib-2008-09/0197.html" or https://bugzilla.redhat.com/show_bug.cgi?id=539809
    // As a side effect, we cannot get timeout working during dns resolution.
    
    curl_easy_setopt_checked( m_curl, CURLOPT_NOSIGNAL, 1 );

    // Disable Nagling to make sure we stay responsive for small requests. 

    curl_easy_setopt_checked( m_curl, CURLOPT_TCP_NODELAY, 1 );

    // Set http 1.0 to not use "transfer-encoding: chunked" which is not supported by
    // Amazon S3.

    curl_easy_setopt_checked( m_curl, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_1_0 );

    // Enable/disable tracing.

    curl_easy_setopt_checked( m_curl, CURLOPT_DEBUGFUNCTION, m_traceCallback );
    curl_easy_setopt_checked( m_curl, CURLOPT_DEBUGDATA, this );
    curl_easy_setopt_checked( m_curl, CURLOPT_VERBOSE, m_traceCallback ? 1L : 0L);

    // Set default write function, otherwise curl will write response to stdout.

    curl_easy_setopt_checked( m_curl, CURLOPT_WRITEFUNCTION, writeNoop );

    // Set CA root certificates.
    
    if(  m_isHttps ) 
    {
        if(  !m_sslCertFile.empty() ) 
        {
            if( !strcmp( m_sslCertFile.c_str(), s_CACertIgnore ) ) 
            {
                curl_easy_setopt_checked( m_curl, CURLOPT_SSL_VERIFYPEER, 0 );
            } 
            else 
            {
              graphlab::fileio::set_ssl_certificate_options(m_curl);
            }
        } 
        else 
        {
          graphlab::fileio::set_ssl_certificate_options(m_curl);
        }
    }

    if( !m_proxy.empty() )
    {
        curl_easy_setopt_checked( m_curl, CURLOPT_PROXY, m_proxy.c_str() );
    }

    // Set request headers.
    
    // Note that the list needs to be available until curl makes actual
    // request, so we cannot have just a local list here to set the headers
    // to curl.

    setRequestHeaders( m_accKey, m_secKey,
        0 /* contentMd5 */, contentType, cacheMaxAge, makePublic, useSrvEncrypt,
        request->httpVerb(), bucketName, key, m_storType,
        &request->headers );

    curl_easy_setopt_checked( m_curl, CURLOPT_HTTPHEADER, static_cast< curl_slist * >( request->headers ) );

    // Prepare the response handler.

    request->prepare( m_curl, m_errorBuffer, sizeof( m_errorBuffer ) );
}

void
WsConnection::init( WsRequest *request, const char *bucketName, const char *key, 
                      const char *keySuffix, const char *contentType, unsigned cacheMaxAge,
                      bool makePublic,  bool useSrvEncrypt )
{
    dbgAssert( bucketName );

    std::string url;
    std::string escapedKey;
    composeUrl( m_baseUrl, bucketName, key, keySuffix, &url, &escapedKey );

    prepare( request, bucketName, key ? escapedKey.c_str() : NULL, contentType, cacheMaxAge, makePublic, useSrvEncrypt );

    request->setUrl( url.c_str() );
}

void 
WsConnection::createBucket( const char *bucketName,  bool makePublic )
{
    dbgAssert( bucketName );
    
    LOG_TRACE( "enter createBucket: conn=0x%llx", ( UInt64 )this );

    try
    {
        // Initialize Put request.

        WsPutRequest request( bucketName );
        init( &request, bucketName, NULL /* key */, NULL /* keySuffix */,
            NULL /* contentType */, c_noCacheControl, makePublic );

        // Construct payload with region name.

        std::string payload;

        if( m_storType != WST_WALRUS && !m_region.empty() )
        {
            payload.reserve( 256 );
            payload.append("<CreateBucketConfiguration><LocationConstraint>");
            payload.append( m_region );
            payload.append( "</LocationConstraint></CreateBucketConfiguration>" );
        }

        request.setUpload( payload.c_str(), payload.size() );

        // Execute and get the response.

        WsResponseDetails &responseDetails = request.execute();  
        handleErrors( responseDetails );
    }
    catch( ... )
    {
        throwSummary( "createBucket", bucketName );
    }

    LOG_TRACE( "leave createBucket: conn=0x%llx", ( UInt64 )this );
}

void
WsConnection::delBucket( const char *bucketName )
{
    dbgAssert( bucketName );

    LOG_TRACE( "enter delBucket: conn=0x%llx", ( UInt64 )this );

    try
    {
        del( bucketName, "" /* key */, NULL /* keySuffix */, NULL /* response */ );
    }
    catch( ... )
    {
        throwSummary( "delBucket", bucketName );
    }

    LOG_TRACE( "leave delBucket: conn=0x%llx", ( UInt64 )this );
}

void             
WsConnection::listAllBuckets( std::vector< WsBucket > *buckets /* out */ )
{
    dbgAssert( buckets );

    LOG_TRACE( "enter listAllBuckets: conn=0x%llx", ( UInt64 )this );

    try
    {
        WsListBucketsRequest request( buckets );
        init( &request, "" /* bucketName */, NULL /* key */, NULL /* keySuffix */ );

        WsResponseDetails &responseDetails = request.execute();  
        handleErrors( responseDetails );
    }
    catch( ... )
    {
        throwSummary( "listAllBuckets", "" );
    }

    LOG_TRACE( "leave listAllBuckets: conn=0x%llx", ( UInt64 )this );
}

static void
completePut( WsResponseDetails &responseDetails, WsPutResponse *response )
{
    handleErrors( responseDetails );

    if( response )
    {
        response->etag.swap( responseDetails.etag );
    }
}

void
WsConnection::put( WsRequest *request, const char *bucketName, const char *key, 
                  const char *uploadId, int partNumber,
                  const char *contentType, unsigned cacheMaxAge,
                  bool makePublic, bool useSrvEncrypt,
                  WsPutResponse *response )
{
    dbgAssert( request );
    dbgAssert( bucketName );
    dbgAssert( implies( uploadId, partNumber) );
    
    std::string keySuffix;
    
    if( uploadId )
    {
        keySuffix.reserve( 256 );
        keySuffix.append( STRING_WITH_LEN( "?partNumber=" ) );
        char partNumberBuf[ 16 ];
        keySuffix.append( uitoa( partNumber, partNumberBuf ) );
        keySuffix.append( STRING_WITH_LEN( "&uploadId=" ) );
        keySuffix.append( uploadId );
    }

    init( request, bucketName, key, uploadId ? keySuffix.c_str() : NULL, 
        contentType ? contentType : s_contentTypeBinary, cacheMaxAge, makePublic, useSrvEncrypt );

    // Execute the request.

    WsResponseDetails &responseDetails = request->execute();  

    // Complete the request.

    ::webstor::completePut( responseDetails, response );
}

void
WsConnection::put( const char *bucketName, const char *key, const void *data, 
    size_t size, const char *contentType, unsigned cacheMaxAge, bool makePublic, bool useSrvEncrypt,
    WsPutResponse *response )
{
    dbgAssert( bucketName );
    dbgAssert( implies( size, data ) );
    dbgAssert( key );

    LOG_TRACE( "enter put: conn=0x%llx", ( UInt64 )this );

    try
    {
        WsPutRequest request( key, data, size );
        put( &request, bucketName, key, NULL /* uploadId */, 0 /* partNumber */,
            contentType, cacheMaxAge, makePublic, useSrvEncrypt, response );
    }
    catch( ... )
    {
        throwSummary( "put", key );
    }

    LOG_TRACE( "leave put: conn=0x%llx", ( UInt64 )this );
}


void 
WsConnection::put( const char *bucketName, const char *key, WsPutRequestUploader *uploader, 
    size_t totalSize, const char *contentType, unsigned cacheMaxAge, bool makePublic, bool useSrvEncrypt,
    WsPutResponse *response )
{
    dbgAssert( bucketName );
    dbgAssert( uploader );
    dbgAssert( key );

    LOG_TRACE( "enter put: conn=0x%llx", ( UInt64 )this );

    try
    {
        WsPutRequest request( key, uploader, totalSize );
        put( &request, bucketName, key, NULL /* uploadId */, 0 /* partNumber */,
            contentType, cacheMaxAge, makePublic, useSrvEncrypt, response );
    }
    catch( ... )
    {
        throwSummary( "put", key );
    }

    LOG_TRACE( "leave put: conn=0x%llx", ( UInt64 )this );
}


void
WsConnection::pendPut( AsyncMan *asyncMan, const char *bucketName, 
                      const char *key, const void *data, size_t size,
                      bool makePublic, bool useSrvEncrypt )
{
    dbgAssert( asyncMan != NULL );
    dbgAssert( bucketName );
    dbgAssert( implies( size, data ) );
    dbgAssert( key );
    dbgAssert( !m_asyncRequest );  // another async operation is in progress.

    LOG_TRACE( "enter pendPut: conn=0x%llx", ( UInt64 )this );

    try
    {
        // Initialize Put request.

        std::auto_ptr< WsPutRequest > request( new WsPutRequest( key, data, size ) );
        init( request.get(), bucketName, key, NULL /* keySuffix */, s_contentTypeBinary, c_noCacheControl, makePublic, useSrvEncrypt );

        // Start async.

        m_curl.pendOp( asyncMan );
        m_asyncRequest = request.release(); // nofail
    }
    catch( ... )
    {
        throwSummary( "pendPut", key );
    }

    LOG_TRACE( "leave pendPut: conn=0x%llx", ( UInt64 )this );
}

void
WsConnection::completePut( WsPutResponse *response )
{
    dbgAssert( m_asyncRequest );
    dbgAssert( dynamic_cast< WsRequest *>( m_asyncRequest ) );

    LOG_TRACE( "enter completePut: conn=0x%llx", ( UInt64 )this );

    // Make sure that async operation is completed no matter what happens below.

    std::auto_ptr< WsRequest > request( static_cast< WsRequest * >( m_asyncRequest ) );
    m_asyncRequest = NULL;

    try
    {
        // Wait till async completes.

        m_curl.completeOp();

        // Complete the request.

        WsResponseDetails &responseDetails = request->complete( static_cast< CURLcode >( m_curl.opResult() ) );
        ::webstor::completePut( responseDetails, response );
    }
    catch( ... )
    {
        throwSummary( "completePut", request->name() );
    }

    LOG_TRACE( "leave completePut: conn=0x%llx", ( UInt64 )this );
}

static void
completeGet( WsResponseDetails &responseDetails, WsGetResponse *response )
{
    // A special case for GetResponse: treat NoSuchKey as success
    // but with loadedContentLength = -1.

    if( responseDetails.status == WS_RESPONSE_STATUS_FAILURE_WITH_DETAILS &&
        ( !strcmp( responseDetails.errorCode.c_str(), "NoSuchKey" ) ||      // Amazon
        !strcmp( responseDetails.errorCode.c_str(), "NoSuchEntity" ) ) )    // Walrus
    {
        responseDetails.status = WS_RESPONSE_STATUS_SUCCESS;
        responseDetails.loadedContentLength = -1;
    }

    handleErrors( responseDetails );

    if( response )
    {
        response->loadedContentLength = responseDetails.loadedContentLength;
        response->isTruncated = responseDetails.isTruncated;
        response->etag.swap( responseDetails.etag );
    }
}

void
WsConnection::get( const char *bucketName, const char *key, WsGetResponseLoader *loader /* in */, 
                    WsGetResponse *response /* out */ )
{
    dbgAssert( bucketName );
    dbgAssert( key );
    dbgAssert( loader );

    LOG_TRACE( "enter get: conn=0x%llx", ( UInt64 )this );

    try
    {
        // Initialize Get request.

        WsGetRequest request( key, loader );
        init( &request, bucketName, key );

        // Execute the request.

        WsResponseDetails &responseDetails = request.execute();  

        // Complete the request.

        ::webstor::completeGet( responseDetails, response );
    }
    catch( ... )
    {
        throwSummary( "get", key );
    }

    LOG_TRACE( "leave get: conn=0x%llx", ( UInt64 )this );
}

void
WsConnection::get( const char *bucketName, const char *key, void *buffer, size_t size, 
                    WsGetResponse *response /* out */ )
{
    dbgAssert( bucketName );
    dbgAssert( key );
    dbgAssert( implies( size, buffer ) );

    WsGetResponseBufferLoader loader( buffer, size );
    get( bucketName, key, &loader, response );
}

void
WsConnection::pendGet( AsyncMan *asyncMan, const char *bucketName, const char *key, void *buffer, size_t size )
{
    dbgAssert( asyncMan != NULL );
    dbgAssert( bucketName );
    dbgAssert( key );
    dbgAssert( implies( size, buffer ) );
    dbgAssert( !m_asyncRequest );  // another async operation is in progress.

    LOG_TRACE( "enter pendGet: conn=0x%llx", ( UInt64 )this );

    try
    {
        // Initialize Get request.

        std::auto_ptr< WsGetRequest > request( new WsGetRequest( key, buffer, size ) );
        init( request.get(), bucketName, key );

        // Start async.

        m_curl.pendOp( asyncMan );
        m_asyncRequest = request.release(); // nofail
    }
    catch( ... )
    {
        throwSummary( "pendGet", key );
    }

    LOG_TRACE( "leave pendGet: conn=0x%llx", ( UInt64 )this );
}

void
WsConnection::completeGet( WsGetResponse *response )
{
    dbgAssert( m_asyncRequest );
    dbgAssert( dynamic_cast< WsGetRequest *>( m_asyncRequest ) );

    LOG_TRACE( "enter completeGet: conn=0x%llx", ( UInt64 )this );

    // Make sure that async operation is completed no matter what happens below.

    std::auto_ptr< WsRequest > request( m_asyncRequest );
    m_asyncRequest = NULL;
    
    try
    {
        // Wait till async completes.

        m_curl.completeOp();

        // Complete the request.

        WsResponseDetails &responseDetails = request->complete( static_cast< CURLcode >( m_curl.opResult() ) );
        ::webstor::completeGet( responseDetails, response );
    }
    catch( ... )
    {
        throwSummary( "completeGet", request->name() );
    }

    LOG_TRACE( "leave completeGet: conn=0x%llx", ( UInt64 )this );
}

void
WsConnection::listObjects( const char *bucketName, const char *prefix,
                        const char *marker, const char *delimiter, unsigned int maxKeys,
                        WsObjectEnum *objectEnum,
                        WsListObjectsResponse *response /* out */ )
{
    dbgAssert( bucketName );
    dbgAssert( objectEnum );

    LOG_TRACE( "enter listObjects: conn=0x%llx", ( UInt64 )this );

    // Workaround for Walrus.

    if( m_storType == WST_WALRUS && ( !marker || !*marker ) )
    {
        marker = " ";
    }

    try
    {
        std::string url;
        url.reserve( 512 );
        url.append( m_baseUrl );
        url.append( bucketName );
        url.append( 1, '/' );

        bool firstQueryPart = true;
        char maxKeysBuf[ 16 ];

        appendQueryPart( &url, "delimiter", delimiter, &firstQueryPart );
        appendQueryPart( &url, "marker", marker, &firstQueryPart );
        appendQueryPart( &url, "max-keys", maxKeys != 0 ? uitoa( maxKeys, maxKeysBuf ) : 0, &firstQueryPart );
        appendQueryPart( &url, "prefix", prefix, &firstQueryPart );

        WsListObjectsRequest request( prefix, objectEnum, m_storType );
        prepare( &request, bucketName, "" /* key */ );

        request.setUrl( url.c_str() ); 

        WsResponseDetails &responseDetails = request.execute();  
        handleErrors( responseDetails );

        if( response )
        {
            response->nextMarker = request.nextMarker();
            response->isTruncated = responseDetails.isTruncated;
        }
    }
    catch( ... )
    {
        throwSummary( "listObjects", bucketName );
    }

    LOG_TRACE( "leave listObjects: conn=0x%llx", ( UInt64 )this );
}

void
WsConnection::listObjects( const char *bucketName, const char *prefix,
    const char *marker, const char *delimiter, unsigned int maxKeys,
    std::vector< WsObject > *objects /* out */,
    WsListObjectsResponse *response /* out */ )
{
    dbgAssert( bucketName );
    dbgAssert( objects );

    struct WsObjectArray : WsObjectEnum
    {
        WsObjectArray( std::vector< WsObject > *_objects )
            : objects( _objects )
        {
            dbgAssert( _objects );
        }

        virtual bool    onObject( const WsObject &object ) 
        {
            objects->push_back( object );
            return true;
        }

        std::vector< WsObject > *objects;
    };

    WsObjectArray objectEnum( objects );
    listObjects( bucketName, prefix, marker, delimiter, maxKeys, &objectEnum, response );
}

void
WsConnection::listAllObjects( const char *bucketName, const char *prefix, 
    const char *delimiter, WsObjectEnum *objectEnum, unsigned int maxKeysInBatch )
{
    LOG_TRACE( "enter listAllObjects: conn=0x%llx", ( UInt64 )this );

    WsListObjectsResponse response;

    do
    {
        listObjects( bucketName, prefix, response.nextMarker.c_str(), delimiter, 
            maxKeysInBatch, objectEnum, &response );
    }
    while( response.isTruncated );

    LOG_TRACE( "enter listAllObjects: conn=0x%llx", ( UInt64 )this );
}

void
WsConnection::listAllObjects( const char *bucketName, const char *prefix, 
    const char *delimiter, std::vector< WsObject> *objects, unsigned int maxKeysInBatch )
{
    LOG_TRACE( "enter listAllObjects: conn=0x%llx", ( UInt64 )this );

    WsListObjectsResponse response;

    do
    {
        listObjects( bucketName, prefix, response.nextMarker.c_str(), delimiter, 
            maxKeysInBatch, objects, &response );
    }
    while( response.isTruncated );

    LOG_TRACE( "enter listAllObjects: conn=0x%llx", ( UInt64 )this );
}

static void
completeDel( WsResponseDetails &responseDetails, WsDelResponse *response )
{
    // A special case for Walrus: handle NoSuchKey as success to be consistent with Amazon.

    if( responseDetails.status == WS_RESPONSE_STATUS_FAILURE_WITH_DETAILS &&
        ( !strcmp( responseDetails.errorCode.c_str(), "NoSuchKey" ) ||      // Amazon
        !strcmp( responseDetails.errorCode.c_str(), "NoSuchEntity" ) ) )    // Walrus
    {
        responseDetails.status = WS_RESPONSE_STATUS_SUCCESS;
    }

    handleErrors( responseDetails );
}

void
WsConnection::del( const char *bucketName, const char *key, const char *keySuffix, 
                  WsDelResponse *response )
{
    dbgAssert( key );

    // Initialize Del request.

    WsDelRequest request( key );
    init( &request, bucketName, key, keySuffix );
   
    // Execute the request.

    WsResponseDetails &responseDetails = request.execute();  

    // Complete the request.

    ::webstor::completeDel( responseDetails, response );
}

void
WsConnection::del( const char *bucketName, const char *key, WsDelResponse *response )
{
    dbgAssert( bucketName );
    dbgAssert( key );

    LOG_TRACE( "enter del: conn=0x%llx", ( UInt64 )this );

    try
    {
        del( bucketName, key, NULL /* keySuffix */, response );
    }
    catch( ... )
    {
        throwSummary( "del", key );
    }

    LOG_TRACE( "leave del: conn=0x%llx", ( UInt64 )this );
}

void
WsConnection::pendDel( AsyncMan *asyncMan, const char *bucketName, const char *key )
{
    dbgAssert( asyncMan != NULL );
    dbgAssert( bucketName );
    dbgAssert( key );
    dbgAssert( !m_asyncRequest );  // another async operation is in progress.

    LOG_TRACE( "enter pendDel: conn=0x%llx", ( UInt64 )this );

    try
    {
        // Initialize Del request.

        std::auto_ptr< WsDelRequest > request( new WsDelRequest( key ) );
        init( request.get(), bucketName, key );

        // Start async.

        m_curl.pendOp( asyncMan );
        m_asyncRequest = request.release(); // nofail
    }
    catch( ... )
    {
        throwSummary( "pendDel", key );
    }

    LOG_TRACE( "leave pendDel: conn=0x%llx", ( UInt64 )this );
}

void
WsConnection::completeDel( WsDelResponse *response )
{
    dbgAssert( m_asyncRequest );
    dbgAssert( dynamic_cast< WsRequest * >( m_asyncRequest ) );

    LOG_TRACE( "enter completeDel: conn=0x%llx", ( UInt64 )this );

    // Make sure that async operation is completed no matter what happens below.

    std::auto_ptr< WsRequest > request( static_cast< WsRequest * >( m_asyncRequest ) );
    m_asyncRequest = NULL;

    try
    {
        // Wait till async completes.

        m_curl.completeOp();

        // Complete the request.

        WsResponseDetails &responseDetails = request->complete( static_cast< CURLcode >( m_curl.opResult() ) );
        ::webstor::completeDel( responseDetails, response );
    }
    catch( ... )
    {
        throwSummary( "completeDel", request->name() );
    }

    LOG_TRACE( "leave completeDel: conn=0x%llx", ( UInt64 )this );
}

void
WsConnection::delAll( const char *bucketName, const char *prefix, unsigned int maxKeysInBatch )
{
    //$ REVIEW: use Amazon batch delete here.

    WsListObjectsResponse response;
    std::vector< WsObject > objects;
    objects.reserve( 64 );

    LOG_TRACE( "enter delAll: conn=0x%llx", ( UInt64 )this );

    do
    {
        listObjects( bucketName, prefix, response.nextMarker.c_str(), 
            NULL /* delimiter*/, maxKeysInBatch, &objects, &response );

        for( int i = 0; i < objects.size(); ++i )
        {
            del( bucketName, objects[i].key.c_str() );
        }
        objects.clear();
    }
    while( response.isTruncated );

    LOG_TRACE( "leave delAll: conn=0x%llx", ( UInt64 )this );
}

void 
WsConnection::initiateMultipartUpload( const char *bucketName, const char *key, 
                        const char *contentType, unsigned cacheMaxAge,
                        bool makePublic, bool useSrvEncrypt,
                        WsInitiateMultipartUploadResponse *response /* out */  )
{
    dbgAssert( bucketName );
    dbgAssert( key );
    dbgAssert( m_storType == WST_S3 );

    LOG_TRACE( "enter initiateMultipartUpload: conn=0x%llx", ( UInt64 )this );

    try
    {
        WsInitiateMultipartUploadRequest request( key );
        init( &request, bucketName, key, "?uploads" /* keySuffix */, 
            contentType ? contentType : s_contentTypeBinary, cacheMaxAge, makePublic, useSrvEncrypt );

        WsResponseDetails &responseDetails = request.execute();  
        handleErrors( responseDetails );

        if( response )
        {
            response->uploadId.swap( responseDetails.uploadId );
        }
    }
    catch( ... )
    {
        throwSummary( "initiateMultipartUpload", key );
    }

    LOG_TRACE( "leave initiateMultipartUpload: conn=0x%llx", ( UInt64 )this );
}

void
WsConnection::putPart( const char *bucketName, const char *key, const char *uploadId, 
        int partNumber, const void *data, size_t size, 
        WsPutResponse *response /* out */  )
{
    dbgAssert( bucketName );
    dbgAssert( key );
    dbgAssert( uploadId );
    dbgAssert( partNumber > 0 );
    dbgAssert( implies( size, data ) );
    dbgAssert( m_storType == WST_S3 );

    LOG_TRACE( "enter putPart: conn=0x%llx", ( UInt64 )this );

    try
    {
        // Note: we don't need to add makePublic and useSrvEncrypt to the header because
        // they are specified in initiateMultipartUpload(..), so pass
        // makePublic=false and useSrvEncrypt=false.

        WsPutRequest request( key, data, size );
        put( &request, bucketName, key, uploadId, partNumber, 
            NULL /* contentType */, c_noCacheControl, false /* makePublic */, false /* useSrvEncrypt */,
            response );
        
        if( response )
        {
            response->partNumber = partNumber;
        }
    }
    catch( ... )
    {
        throwSummary( "putPart", key );
    }

    LOG_TRACE( "leave putPart: conn=0x%llx", ( UInt64 )this );
}


void
WsConnection::putPart( const char *bucketName, const char *key, const char *uploadId, 
        int partNumber, WsPutRequestUploader *uploader, size_t partSize, 
        WsPutResponse *response /* out */  )
{
    dbgAssert( bucketName );
    dbgAssert( key );
    dbgAssert( uploadId );
    dbgAssert( partNumber > 0 );
    dbgAssert( uploader );
    dbgAssert( m_storType == WST_S3 );

    LOG_TRACE( "enter putPart: conn=0x%llx", ( UInt64 )this );

    try
    {
        // Note: we don't need to add makePublic and useSrvEncrypt to the header because
        // they are specified in initiateMultipartUpload(..), so pass
        // makePublic=false and useSrvEncrypt=false.

        WsPutRequest request( key, uploader, partSize );
        put( &request, bucketName, key, uploadId, partNumber, 
            NULL /* contentType */, c_noCacheControl, false /* makePublic */, false /* useSrvEncrypt */,
            response );
        
        if( response )
        {
            response->partNumber = partNumber;
        }
    }
    catch( ... )
    {
        throwSummary( "putPart", key );
    }

    LOG_TRACE( "leave putPart: conn=0x%llx", ( UInt64 )this );
}

void 
WsConnection::completeMultipartUpload( const char *bucketName, const char *key, 
                        const char *uploadId, const WsPutResponse *parts, size_t size, 
                        WsCompleteMultipartUploadResponse *response /* out */ )
{
    dbgAssert( bucketName );
    dbgAssert( key );
    dbgAssert( uploadId );
    dbgAssert( parts );
    dbgAssert( m_storType == WST_S3 );

    LOG_TRACE( "enter completeMultipartUpload: conn=0x%llx", ( UInt64 )this );

    try
    {
        WsCompleteMultipartUploadRequest request( key );

        std::string keySuffix;
        keySuffix.reserve( 256 );
        keySuffix.append( STRING_WITH_LEN( "?uploadId=" ) );
        keySuffix.append( uploadId );

        init( &request, bucketName, key, keySuffix.c_str(), s_contentTypeBinary );

        // Construct POST request.

        std::string postRequest;
        postRequest.reserve( 1024 );
        postRequest.append( STRING_WITH_LEN( "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" ) );
        postRequest.append( STRING_WITH_LEN( "<CompleteMultipartUpload>" ) );

        for( size_t i = 0; i < size; ++i )
        {
            postRequest.append( STRING_WITH_LEN( "<Part>" ) );
            postRequest.append( STRING_WITH_LEN( "<PartNumber> " ) );
            char partNumberBuf[ 16 ];
            postRequest.append( uitoa( parts[ i ].partNumber, partNumberBuf ) );
            postRequest.append( STRING_WITH_LEN( "</PartNumber>" ) );
            postRequest.append( STRING_WITH_LEN( "<ETag>\"" ) );
            postRequest.append( parts[ i ].etag );
            postRequest.append( STRING_WITH_LEN( "\"</ETag>" ) );
            postRequest.append( STRING_WITH_LEN( "</Part>" ) );
        }

        postRequest.append( STRING_WITH_LEN( "</CompleteMultipartUpload>" ) );

        request.setUpload( postRequest.c_str(), postRequest.size() );

        WsResponseDetails &responseDetails = request.execute();  
        handleErrors( responseDetails );

        if( response )
        {
            response->etag.swap( responseDetails.etag );
        }
    }
    catch( ... )
    {
        throwSummary( "completeMultipartUpload", key );
    }

    LOG_TRACE( "leave completeMultipartUpload: conn=0x%llx", ( UInt64 )this );
}

void
WsConnection::abortMultipartUpload( const char *bucketName, const char *key, const char *uploadId, 
                                    WsDelResponse *response /* out */ )
{
    dbgAssert( bucketName );
    dbgAssert( key );
    dbgAssert( uploadId );
    dbgAssert( m_storType == WST_S3 );

    LOG_TRACE( "enter abortMultipartUpload: conn=0x%llx", ( UInt64 )this );

    try
    {
        std::string keySuffix;
        keySuffix.reserve( 256 );
        keySuffix.append( STRING_WITH_LEN( "?uploadId=" ) );
        keySuffix.append( uploadId );

        del( bucketName, key, keySuffix.c_str(), response );
    }
    catch( ... )
    {
        throwSummary( "abortMultipartUpload", key );
    }

    LOG_TRACE( "leave abortMultipartUpload: conn=0x%llx", ( UInt64 )this );
}

void
WsConnection::abortAllMultipartUploads( const char *bucketName, const char *prefix,
    unsigned int maxUploadsInBatch )
{
    WsListMultipartUploadsResponse response;
    std::vector< WsMultipartUpload > uploads;
    uploads.reserve( 64 );

    LOG_TRACE( "enter abortAllMultipartUploads: conn=0x%llx", ( UInt64 )this );

    do
    {
        listMultipartUploads( bucketName, prefix, response.nextKeyMarker.c_str(), 
            response.nextUploadIdMarker.c_str(), 
            NULL /* delimiter*/, maxUploadsInBatch, &uploads, &response );

        for( int i = 0; i < uploads.size(); ++i )
        {
            abortMultipartUpload( bucketName, uploads[i].key.c_str(), 
                uploads[i].uploadId.c_str() );
        }
        uploads.clear();
    }
    while( response.isTruncated );

    LOG_TRACE( "leave abortAllMultipartUploads: conn=0x%llx", ( UInt64 )this );
}

void
WsConnection::listMultipartUploads( const char *bucketName, const char *prefix,
                    const char *keyMarker, const char *uploadIdMarker, const char *delimiter, 
                    unsigned int maxUploads,
                    WsMultipartUploadEnum *uploadEnum,
                    WsListMultipartUploadsResponse *response /* out */ )
{
    dbgAssert( bucketName );
    dbgAssert( uploadEnum );
    dbgAssert( m_storType == WST_S3 );

    LOG_TRACE( "enter listMultipartUploads: conn=0x%llx", ( UInt64 )this );

    try
    {
        std::string url;
        url.reserve( 512 );
        url.append( m_baseUrl );
        url.append( bucketName );
        url.append( STRING_WITH_LEN( "/?uploads" ) );

        char maxUploadsBuf[ 16 ];

        appendQueryPart( &url, "delimiter", delimiter );
        appendQueryPart( &url, "key-marker", keyMarker );
        appendQueryPart( &url, "max-uploads", maxUploads != 0 ? uitoa( maxUploads, maxUploadsBuf ) : 0 );
        appendQueryPart( &url, "prefix", prefix );
        appendQueryPart( &url, "upload-id-marker", uploadIdMarker );

        WsListMultipartUploadsRequest request( prefix, uploadEnum );
        prepare( &request, bucketName, "?uploads" /* key */ );

        request.setUrl( url.c_str() );

        WsResponseDetails &responseDetails = request.execute();  
        handleErrors( responseDetails );

        if( response )
        {
            response->nextKeyMarker = request.lastUpload().key;
            response->nextUploadIdMarker = request.lastUpload().uploadId;
            response->isTruncated = responseDetails.isTruncated;
        }
    }
    catch( ... )
    {
        throwSummary( "listMultipartUploads", prefix ? prefix : "" );
    }

    LOG_TRACE( "leave listMultipartUploads: conn=0x%llx", ( UInt64 )this );
}

void
WsConnection::listMultipartUploads( const char *bucketName, const char *prefix,
                    const char *keyMarker, const char *uploadIdMarker, const char *delimiter, 
                    unsigned int maxUploads,
                    std::vector< WsMultipartUpload > *uploads /* out */,
                    WsListMultipartUploadsResponse *response /* out */ )
{
    dbgAssert( bucketName );
    dbgAssert( uploads );
    dbgAssert( m_storType == WST_S3 );

    struct WsMultipartUploadArray : WsMultipartUploadEnum
    {
        WsMultipartUploadArray( std::vector< WsMultipartUpload > *_uploads )
            : uploads( _uploads )
        {
            dbgAssert( _uploads );
        }

        virtual bool    onUpload( const WsMultipartUpload &upload ) 
        {
            uploads->push_back( upload );
            return true;
        }

        std::vector< WsMultipartUpload > *uploads;
    };

    WsMultipartUploadArray uploadEnum( uploads );
    listMultipartUploads( bucketName, prefix, keyMarker, uploadIdMarker, delimiter, maxUploads, 
        &uploadEnum, response );
}

void
WsConnection::listAllMultipartUploads( const char *bucketName, const char *prefix, 
                    const char *delimiter, 
                    WsMultipartUploadEnum *uploadEnum, 
                    unsigned int maxUploadsInBatch )
{
    dbgAssert( uploadEnum );

    LOG_TRACE( "enter listAllMultipartUploads: conn=0x%llx", ( UInt64 )this );

    WsListMultipartUploadsResponse response;

    do
    {
        listMultipartUploads( bucketName, prefix, response.nextKeyMarker.c_str(), 
            response.nextUploadIdMarker.c_str(), 
            delimiter, maxUploadsInBatch, uploadEnum, &response );
    }
    while( response.isTruncated );

    LOG_TRACE( "leave listAllMultipartUploads: conn=0x%llx", ( UInt64 )this );
}

void
WsConnection::listAllMultipartUploads( const char *bucketName, const char *prefix, 
                       const char *delimiter, std::vector< WsMultipartUpload > *uploads, 
                       unsigned int maxUploadsInBatch )
{
    dbgAssert( uploads );

    LOG_TRACE( "enter listAllMultipartUploads: conn=0x%llx", ( UInt64 )this );

    WsListMultipartUploadsResponse response;

    do
    {
        listMultipartUploads( bucketName, prefix, response.nextKeyMarker.c_str(), 
            response.nextUploadIdMarker.c_str(), 
            delimiter, maxUploadsInBatch, uploads, &response );
    }
    while( response.isTruncated );

    LOG_TRACE( "leave listAllMultipartUploads: conn=0x%llx", ( UInt64 )this );
}

void
WsConnection::setTimeout( long timeout )
{
    m_timeout = timeout;
}

void
WsConnection::setConnectTimeout( long connectTime )
{
    m_connectTimeout = connectTime;
}

//////////////////////////////////////////////////////////////////////////////
// WsException.

WsException::WsException( const char *fmt, ... )
{
    dbgAssert( fmt );

    m_msg.resize( 512 );

    va_list args;
    va_start( args, fmt );
    vsnprintf( &m_msg[ 0 ], m_msg.size() - 1, fmt, args );  // -1 to leave room for \0 on Windows.
    va_end( args );
}

const char *
WsException::what() const throw() 
{ 
    return &m_msg[ 0 ]; 
};

//////////////////////////////////////////////////////////////////////////////
// Debugging support.

#ifdef DEBUG
void
dbgSetShowAssert( ::webstor::dbgShowAssertFunc *callback )
{
    ::webstor::internal::g_dbgShowAssertFunc_ = callback;
}
#endif

}  // namespace webstor
