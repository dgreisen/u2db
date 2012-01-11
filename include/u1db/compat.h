/*
 * Copyright 2011-2012 Canonical Ltd.
 *
 * This file is part of u1db.
 *
 * u1db is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License version 3
 * as published by the Free Software Foundation.
 *
 * u1db is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with u1db.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef U1DB_COMPAT_H
#define U1DB_COMPAT_H

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

#endif /* U1DB_COMPAT_H */

