#!/usr/bin/env python
# Copyright 2011 Canonical Ltd.
#
# This program is free software: you can redistribute it and/or modify it
# under the terms of the GNU General Public License version 3, as published
# by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranties of
# MERCHANTABILITY, SATISFACTORY QUALITY, or FITNESS FOR A PARTICULAR
# PURPOSE.  See the GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program.  If not, see <http://www.gnu.org/licenses/>.

def config():
    import u1db
    ext = []
    kwargs = {
        "name": "u1db",
        "version": u1db.__version__,
        "description": "Simple syncable document storage",
        "url": "https://launchpad.net/u1db",
        "license": "GNU GPL v3",
        "download_url": "https://launchpad.net/u1db/+download",
        "packages": ["u1db"],
        "scripts": [],
        "ext_modules": ext,
        "classifiers": [
            'Development Status :: 4 - Beta',
            'Environment :: Console',
            'Intended Audience :: Developers',
            'License :: OSI Approved :: GNU General Public License (GPL)',
            'Operating System :: OS Independent',
            'Operating System :: Microsoft :: Windows',
            'Operating System :: POSIX',
            'Programming Language :: Python',
            'Programming Language :: Cython',
            'Topic :: Software Development :: Debuggers',
        ],
        "long_description": """\
A simple syncable JSON document store.

This allows you to get, retrieve, index, and update JSON documents, and
synchronize them with other stores.
"""
    }

    from distutils.core import setup, Extension

    try:
        from Cython.Distutils import build_ext
    except ImportError:
        print "We require Cython to be installed."
        return

    kwargs["cmdclass"] = {"build_ext": build_ext}
    ext.append(Extension("u1db.backends.c_wrapper",
                         ["u1db/backends/c_wrapper.pyx",
                          "../c/u1db.c"],
                         include_dirs=["../c"]))

    setup(**kwargs)

if __name__ == "__main__":
    config()
