/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef CPPIPC_SERVER_CONSOLE_CANCEL_HANDLER_UNIX_HPP
#define CPPIPC_SERVER_CONSOLE_CANCEL_HANDLER_UNIX_HPP

#include <csignal>
#include <cppipc/client/console_cancel_handler.hpp>

namespace cppipc {

class console_cancel_handler_unix : public console_cancel_handler {
 public:
  typedef console_cancel_handler super;
  bool set_handler();
  bool unset_handler();
  void raise_cancel();
  console_cancel_handler_unix();

 private:

  struct sigaction m_sigint_act;
  struct sigaction m_prev_sigint_act;
};

} // namespace cppipc

#endif //CPPIPC_SERVER_CONSOLE_CANCEL_HANDLER_UNIX_HPP
