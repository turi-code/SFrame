/*
* Copyright (C) 2015 Dato, Inc.
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Affero General Public License as
* published by the Free Software Foundation, either version 3 of the
* License, or (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU Affero General Public License for more details.
*
* You should have received a copy of the GNU Affero General Public License
* along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/
#ifndef GRAPHLAB_TEST_LAMBDA_DUMMY_WORKER_INTERFACE
#define GRAPHLAB_TEST_LAMBDA_DUMMY_WORKER_INTERFACE

#include <cppipc/cppipc.hpp>

GENERATE_INTERFACE_AND_PROXY(dummy_worker_interface, dummy_worker_proxy,
      (std::string, echo, (const std::string&))
      (void, throw_error, )
      (void, quit, (int))
    );

#endif
