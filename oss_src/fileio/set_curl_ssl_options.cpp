#include <fileio/fileio_constants.hpp>
#include <logger/assertions.hpp>
extern "C" {
#include <curl/curl.h>
}

namespace graphlab {
namespace fileio {

void set_ssl_certificate_options(void* ecurl) {
  using graphlab::fileio::get_alternative_ssl_cert_dir;
  using graphlab::fileio::get_alternative_ssl_cert_file;
  using graphlab::fileio::disable_ssl_cert_checks;
  if (!get_alternative_ssl_cert_dir().empty()) {
    ASSERT_EQ(curl_easy_setopt((CURL*)ecurl, CURLOPT_CAPATH, get_alternative_ssl_cert_dir().c_str()), CURLE_OK);
  }
  if (!get_alternative_ssl_cert_file().empty()) {
    ASSERT_EQ(curl_easy_setopt((CURL*)ecurl, CURLOPT_CAINFO, get_alternative_ssl_cert_file().c_str()), CURLE_OK);
  }
  if (disable_ssl_cert_checks()) {
    ASSERT_EQ(curl_easy_setopt((CURL*)ecurl, CURLOPT_SSL_VERIFYPEER, 0l), CURLE_OK);
    ASSERT_EQ(curl_easy_setopt((CURL*)ecurl, CURLOPT_SSL_VERIFYHOST, 0l), CURLE_OK);
  }
}

} // fileio
} // graphlab
