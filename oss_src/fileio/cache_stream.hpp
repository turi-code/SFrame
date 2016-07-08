/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef GRAPHLAB_FILEIO_CACHE_STREAM_HPP
#define GRAPHLAB_FILEIO_CACHE_STREAM_HPP

#include<fileio/cache_stream_source.hpp>
#include<fileio/cache_stream_sink.hpp>

namespace graphlab {
namespace fileio{

typedef boost::iostreams::stream<graphlab::fileio_impl::cache_stream_source>
  icache_stream;

typedef boost::iostreams::stream<graphlab::fileio_impl::cache_stream_sink>
  ocache_stream;

} // end of fileio
} // end of graphlab

#endif
