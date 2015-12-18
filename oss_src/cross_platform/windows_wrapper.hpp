#ifndef GRAPHLAB_GL_WINDOWS_H_
#define GRAPHLAB_GL_WINDOWS_H_

// Provides a consistent way to include windows.h.  We need to include
// the winsock2.h header first, as using the outdated winsock.h header
// can cause issues when linking against libraries that use the new
// winsock2.h header (introduced in Windows 95).  windows.h includes
// this by default.

#ifdef _WIN32
#include <winsock2.h>
#include <windows.h>
#endif

#endif /* _GL_WINDOWS_H_ */
