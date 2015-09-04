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
#include <crash_handler/crash_handler.hpp>
#include <cstdlib>
#include <cstring>

void crash() {
  int *a = (int*)-1; // make a bad pointer
  printf("%d\n", *a);       // causes segfault
}

void bar() {
  crash();
}

void foo() {
  bar();
}

int main(int argc, char** argv) {
  struct sigaction sigact;
  sigact.sa_sigaction = crit_err_hdlr;
  sigact.sa_flags = SA_RESTART | SA_SIGINFO;
  if (sigaction(SIGSEGV, &sigact, (struct sigaction *)NULL) != 0)
  {
    fprintf(stderr, "error setting signal handler for %d (%s)\n",
        SIGSEGV, strsignal(SIGSEGV));
    exit(EXIT_FAILURE);
  }

  foo();

  return 0;
}
