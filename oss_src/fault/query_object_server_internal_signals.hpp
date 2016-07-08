/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef QUERY_OBJECT_SERVER_INTERNAL_SIGNALS_HPP
#define QUERY_OBJECT_SERVER_INTERNAL_SIGNALS_HPP
// this defines the set of messages that are passed from the manager to the
// server processes.

#define QO_SERVER_FAIL -1
#define QO_SERVER_STOP 0
#define QO_SERVER_PROMOTE 1
#define QO_SERVER_PRINT 2


#define QO_SERVER_FAIL_STR "-1\n"
#define QO_SERVER_STOP_STR "0\n"
#define QO_SERVER_PROMOTE_STR "1\n"
#define QO_SERVER_PRINT_STR "2\n"


#endif
