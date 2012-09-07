cmake_minimum_required (VERSION 2.6)
project (u1db)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/")
include_directories(${CMAKE_SOURCE_DIR}/include)

execute_process(COMMAND python ${CMAKE_SOURCE_DIR}/setup.py --version
                OUTPUT_VARIABLE PROJECT_VERSION OUTPUT_STRIP_TRAILING_WHITESPACE)

set(ARCHIVE_NAME ${CMAKE_PROJECT_NAME}-${PROJECT_VERSION})
add_custom_target(dist
    COMMAND bzr export --root=${ARCHIVE_NAME}
        ${CMAKE_BINARY_DIR}/${ARCHIVE_NAME}.tar.bz2
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})

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

add_custom_target(doctests
    COMMAND cd html-docs && make doctest && cd ..
)

add_custom_target(check-nodoc
    COMMAND python -m testtools.run discover
    DEPENDS build-inplace-for-tests
)

add_custom_target(check
    COMMAND python -m testtools.run discover
    DEPENDS build-inplace-for-tests doctests
)

add_custom_target(cdocs
    COMMAND doxygen html-docs/doxygen.conf
)

add_custom_target(build-inplace
    COMMAND python setup.py build_ext -i -f -I ${CMAKE_SOURCE_DIR}/include
    DEPENDS ReplicatePythonSourceTree u1db
)

add_custom_target(build-debug
    COMMAND python-dbg setup.py build_ext -i -f -I ${CMAKE_SOURCE_DIR}/include
    DEPENDS ReplicatePythonSourceTree u1db
)

add_custom_target(build-inplace-for-tests
    COMMAND U1DB_TEST=1 python setup.py build_ext -i -f -I ${CMAKE_SOURCE_DIR}/include
    DEPENDS ReplicatePythonSourceTree u1db
)

add_custom_target(build-debug-for-tests
    COMMAND U1DB_TEST=1 python-dbg setup.py build_ext -i -f -I ${CMAKE_SOURCE_DIR}/include
    DEPENDS ReplicatePythonSourceTree u1db
)
add_custom_target(check-valgrind
    COMMAND valgrind --tool=memcheck --suppressions=${CMAKE_SOURCE_DIR}/custom.supp python-dbg -m testtools.run discover
    DEPENDS build-debug-for-tests
)

add_custom_target(check-valgrind-leaks
    COMMAND valgrind --tool=memcheck --suppressions=${CMAKE_SOURCE_DIR}/custom.supp --track-origins=yes --num-callers=40 --leak-resolution=high --leak-check=full python-dbg -m testtools.run discover
    DEPENDS build-debug-for-tests
)

add_custom_target(check-verbose
    COMMAND U1DB_TEST=1 python -c \"import unittest, sys\; from testtools import run\; run.TestProgram(argv=sys.argv, testRunner=unittest.TextTestRunner(verbosity=2), stdout=sys.stdout)\" discover
    DEPENDS build-inplace-for-tests
)

add_custom_target(html-docs
    COMMAND cd ${CMAKE_SOURCE_DIR}/html-docs; make html
)

SET(CPACK_GENERATOR "TGZ")
SET(CPACK_DEBIAN_PACKAGE_MAINTAINER "Noone")
INCLUDE(CPack)

add_custom_target(install-python ALL
    # Do NOT build this one in-place
    COMMAND python setup.py build_ext -f -I ${CMAKE_SOURCE_DIR}/include
    COMMAND python setup.py install --prefix=${CMAKE_INSTALL_PREFIX} --root=${CMAKE_CURRENT_BINARY_DIR}/temp
    DEPENDS ReplicatePythonSourceTree u1db
)

install(DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/temp/${CMAKE_INSTALL_PREFIX}/
    DESTINATION "."
    PATTERN "*.pyc" EXCLUDE
    PATTERN "tests" EXCLUDE
    PATTERN "u1db-client"
    PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ GROUP_EXECUTE GROUP_READ WORLD_EXECUTE WORLD_READ
    PATTERN "u1db-server"
    PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ GROUP_EXECUTE GROUP_READ WORLD_EXECUTE WORLD_READ
)

add_subdirectory(src)
