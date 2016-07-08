/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
/*
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


#ifndef GRAPHLAB_DC_COMPILE_PARAMETERS_HPP
#define GRAPHLAB_DC_COMPILE_PARAMETERS_HPP

// do not change
/**
  \ingroup rpc
  \def RPC_DEFAULT_COMMTYPE
  \brief default communication method
 */
#define RPC_DEFAULT_COMMTYPE TCP_COMM

/**
  \ingroup rpc
  \def RPC_MAX_N_PROCS
  \brief Maximum number of processes supported
 */
#define RPC_MAX_N_PROCS 128

/**
 * \ingroup RPC
 * \def RECEIVE_BUFFER_SIZE
 * The size of the receive buffer for each socket
 */
#define RECEIVE_BUFFER_SIZE 131072

/**************************************************************************/
/*                                                                        */
/*                      Send Buffer Behavior Control                      */
/*                                                                        */
/**************************************************************************/

/*
 * The architecture of the sending subsystem is that there is 1 main send thread.
 * Which polls a collection of thread local queues.
 *
 * Each thread local send queues comprises of 1 queue for each target machine.
 * Each queue comprises of 2 parts:
 *  - An array of "full" buffers
 *  - One not-full buffer.
 */ 

/**
 * \ingroup RPC
 * \def SEND_POLL_TIMEOUT
 * The TCP sender polls the queues every so often to ensure
 * progress; This is the timeout value for the number of microseconds
 * between each poll.
 */
#define SEND_POLL_TIMEOUT 5000


/**
 * \ingroup rpc
 * \def INITIAL_BUFFER_SIZE
 * Each buffer is allocated to this size at the start
 */
#define INITIAL_BUFFER_SIZE RECEIVE_BUFFER_SIZE


/**
 * \ingroup rpc
 * \def FULL_BUFFER_SIZE_LIMIT
 * Once the buffer contents exceeds this, it becomes a full buffer.
 */
#define FULL_BUFFER_SIZE_LIMIT (16*1048576)

/**
 * \ingroup RPC
 * \def NUM_FULL_BUFFER_LIMIT 
 * Number of full buffers in the send queue before a flush is explicitly called.
 */
#define NUM_FULL_BUFFER_LIMIT 64

/**************************************************************************/
/*                                                                        */
/*                          RPC Handling Control                          */
/*                                                                        */
/**************************************************************************/

/**
  \ingroup rpc
  \def RPC_DEFAULT_NUMHANDLERTHREADS
  \brief default number of handler threads to spawn.
 */
#define RPC_DEFAULT_NUMHANDLERTHREADS (size_t)(-1)

/**
 * \ingroup RPC
 * \def RPC_DO_NOT_BREAK_BLOCKS
 *
 * If this option is turned on,
 * collections of messages recieved in a buffer 
 * will all be executed by the same thread. 
 * This decreases latency and increases throughput
 * but at a cost of parallelism.
 * Also, if turned on together with RPC_BLOCK_STRIPING,
 * the sequentialization key is ignored.
 */
#define RPC_DO_NOT_BREAK_BLOCKS


/**
 * \ingroup RPC
 * \def RPC_BLOCK_STRIPING
 * Incoming buffers are striped across threads 
 * to be processed. If this is turned on together with
 * RPC_DO_NOT_BREAK_BLOCKS, the sequentialization key is
 * ignored.
 */
#define RPC_BLOCK_STRIPING

/**************************************************************************/
/*                                                                        */
/*                             Miscellaneous                              */
/*                                                                        */
/**************************************************************************/
/**
 * \ingroup RPC
 * \def DEFAULT_BUFFERED_EXCHANGE_SIZE
 * maximum size of each buffer in the buffer exchange. Beyond this size,
 * a send is performed.
 */
#define DEFAULT_BUFFERED_EXCHANGE_SIZE FULL_BUFFER_SIZE_LIMIT

#define DISABLE_EVENT_LOG
#endif
