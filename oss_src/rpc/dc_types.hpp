/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
/**
 * Copyright (c) 2009 Carnegie Mellon University.
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS
 *  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied.  See the License for the specific language
 *  governing permissions and limitations under the License.
 *
 * For more about this software visit:
 *
 *      http://www.graphlab.ml.cmu.edu
 *
 */


#ifndef DISTRIBUTED_CONTROL_TYPES_HPP
#define DISTRIBUTED_CONTROL_TYPES_HPP
#include <inttypes.h>
#include <serialization/iarchive.hpp>
namespace graphlab {
  /// The type used for numbering processors \ingroup rpc
  typedef uint16_t procid_t;

  /**
   * \internal
   * \ingroup rpc
   * The underlying communication protocol
   */
  enum dc_comm_type {
    TCP_COMM,   ///< TCP/IP
    SCTP_COMM   ///< SCTP (limited support)
  };


  /**
   * \internal
   * \ingroup rpc
   * A pointer that points directly into
   * the middle of a deserialized buffer.
   */
  struct wild_pointer {
    const void* ptr;

    void load(iarchive& iarc) {
      assert(iarc.buf != NULL);
      ptr = reinterpret_cast<const void*>(iarc.buf + iarc.off);
    }
  };
};
#include <rpc/dc_packet_mask.hpp>
#endif

