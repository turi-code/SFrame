/**
 * Copyright (C) 2015 Dato, Inc.
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license. See the LICENSE file for details.
 */
#include <cppipc/client/console_cancel_handler_win.hpp>
#include <cppipc/client/comm_client.hpp>
#include <export.hpp>
#include <windows.h>

namespace cppipc {

BOOL cancel_handler(DWORD dwCtrlType) {
  if(dwCtrlType == CTRL_C_EVENT || dwCtrlType == CTRL_BREAK_EVENT) {
    console_cancel_handler::get_instance().set_cancel_flag(true);
    auto &c = get_cancelled_command();
    auto &r = get_running_command();
    c.store(r.load());

    return TRUE;
  }

  return FALSE;
}

EXPORT console_cancel_handler& console_cancel_handler::get_instance() {
  static console_cancel_handler_win instance;
  return instance;
}

bool console_cancel_handler_win::set_handler() {
  //TODO: Why don't I have to take the address of cancel_handler?
  if(!SetConsoleCtrlHandler((PHANDLER_ROUTINE)cancel_handler, TRUE))
    return false;

  return true;
}

bool console_cancel_handler_win::unset_handler() {
  if(!SetConsoleCtrlHandler((PHANDLER_ROUTINE)cancel_handler, FALSE))
    return false;

  return true;
}

void console_cancel_handler_win::raise_cancel() {
  // CTRL_C_EVENT can be turned off easily.  CTRL_BREAK_EVENT can't I think.
  GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT, 0);
}

} // namespace cppipc
