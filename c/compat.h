/*
 * Copyright 2011 Canonical Ltd.
 * 
 * This program is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 3, as published
 * by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranties of
 * MERCHANTABILITY, SATISFACTORY QUALITY, or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along
 * with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef COMPAT_H
#define COMPAT_H

#if defined(_WIN32) || defined(WIN32)
    /* Windows likes to complain when you use stuff like 'snprintf'
     * Disable that
     */
    #define _CRT_SECURE_NO_WARNINGS
    /* Defining WIN32_LEAN_AND_MEAN makes including windows quite a bit
     * lighter weight.
     */
    #define WIN32_LEAN_AND_MEAN
    #include <windows.h>

#endif

#include <stdio.h>

#ifdef _MSC_VER
#define  snprintf  _snprintf
/* gcc (mingw32) has strtoll, while the MSVC compiler uses _strtoi64 */
#define strtoll _strtoi64
#define strtoull _strtoui64
#define strdup _strdup
#define strndup _win32_strndup
#endif

/* Introduced in Python 2.6 */
#ifndef Py_TYPE
#  define Py_TYPE(o) ((o)->ob_type)
#endif
#ifndef Py_REFCNT
#  define Py_REFCNT(o) ((o)->ob_refcnt)
#endif

#endif /* COMPAT_H */

