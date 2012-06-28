cmake_minimum_required (VERSION 2.6)
project (u1db)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/")
include_directories(${CMAKE_SOURCE_DIR}/include)

find_package(Cython REQUIRED)

find_package(CURL REQUIRED)
if (CURL_FOUND)
  include_directories(${CURL_INCLUDE_DIRS})
endif (CURL_FOUND)

find_package(OAUTH REQUIRED)
if (OAUTH_FOUND)
  include_directories(${OAUTH_INCLUDE_DIRS})
endif (OAUTH_FOUND)

find_package(JSON REQUIRED)
if (JSON_FOUND)
  include_directories(${JSON_INCLUDE_DIRS})
endif (JSON_FOUND)

find_package(Sqlite3 REQUIRED)
if (Sqlite3_FOUND)
  include_directories(${Sqlite3_INCLUDE_DIRS})
endif (Sqlite3_FOUND)

add_custom_target( ReplicatePythonSourceTree ALL ${CMAKE_COMMAND} -P
  ${CMAKE_CURRENT_SOURCE_DIR}/cmake/Modules/ReplicatePythonSourceTree.cmake
  ${CMAKE_CURRENT_BINARY_DIR}
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} )

add_custom_target(check
    COMMAND python -m testtools.run discover
    DEPENDS build-inplace
)

add_custom_target(build-inplace
    COMMAND python setup.py build_ext -i
    DEPENDS ReplicatePythonSourceTree u1db
)

add_custom_target(build-debug
    COMMAND python-dbg setup.py build_ext -i
    DEPENDS ReplicatePythonSourceTree u1db
)

add_custom_target(check-valgrind
    COMMAND valgrind --tool=memcheck --suppressions=${CMAKE_SOURCE_DIR}/custom.supp python-dbg -m testtools.run discover
    DEPENDS build-debug
)

add_custom_target(check-valgrind-leaks
    COMMAND valgrind --tool=memcheck --suppressions=${CMAKE_SOURCE_DIR}/custom.supp --track-origins=yes --num-callers=40 --leak-resolution=high --leak-check=full python-dbg -m testtools.run discover
    DEPENDS build-debug
)

add_custom_target(check-verbose
    COMMAND python -c \"import unittest, sys\; from testtools import run\; run.TestProgram(argv=sys.argv, testRunner=unittest.TextTestRunner(verbosity=2), stdout=sys.stdout)\" discover
    DEPENDS build-inplace
)

add_custom_target(html-docs
    COMMAND cd ${CMAKE_SOURCE_DIR}/html-docs; make html
)

add_custom_target(install-python
    COMMAND python setup.py install --prefix=${CMAKE_INSTALL_PREFIX}
    DEPENDS ReplicatePythonSourceTree
)

add_subdirectory(src)