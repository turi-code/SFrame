/**
 * Copyright (C) 2016 Turi
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#ifndef CPPIPC_SERVER_CONSOLE_CANCEL_HANDLER_WIN_HPP
#define CPPIPC_SERVER_CONSOLE_CANCEL_HANDLER_WIN_HPP

#include <cppipc/client/console_cancel_handler.hpp>

namespace cppipc {

class console_cancel_handler_win : public console_cancel_handler {
 public:
  typedef console_cancel_handler super;

  bool set_handler();
  bool unset_handler();
  void raise_cancel();
  console_cancel_handler_win() : super() {}
 private:

};
} // namespace cppipc


#endif //CPPIPC_SERVER_CONSOLE_CANCEL_HANDLER_WIN_HPP
