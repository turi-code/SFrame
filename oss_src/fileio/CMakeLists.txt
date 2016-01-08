project(fileio)

if(WIN32)
  SET(PLATFORM_DEPENDENCIES ws2_32)
else()
  SET(PLATFORM_DEPENDENCIES dl)
endif()


if (HAS_HDFS)
        add_library(soft_hdfs INTERFACE )
        target_compile_definitions(soft_hdfs INTERFACE HAS_HDFS)
        target_compile_definitions(soft_hdfs INTERFACE HAS_HADOOP) # legacy
endif()

make_library(fileio
  SOURCES
    oss_webstor/asyncurl.cpp
    oss_webstor/sysutils.cpp
    oss_webstor/wsconn.cpp
    s3_api.cpp
    run_aws.cpp
    hdfs.cpp
    libhdfs_shim.cpp
    union_fstream.cpp
    general_fstream_source.cpp
    general_fstream_sink.cpp
    general_fstream.cpp
    cache_stream_source.cpp
    cache_stream_sink.cpp
    fixed_size_cache_manager.cpp
    temp_files.cpp
    curl_downloader.cpp
    sanitize_url.cpp
    file_download_cache.cpp
    fs_utils.cpp
    file_handle_pool.cpp
    fileio_constants.cpp
    s3_fstream.cpp
    block_cache.cpp
    set_curl_options.cpp
    dmlcio/s3_filesys.cc
  REQUIRES
    curl openssl libxml2 logger pthread z cancel_serverside_ops globals process util soft_hdfs ${PLATFORM_DEPENDENCIES} network random
  MAC_REQUIRES
    iconv
  )
