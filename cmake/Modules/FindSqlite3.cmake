# - Try to find Sqlite3
# Once done, this will define
#
#  Sqlite3_FOUND - system has Sqlite3
#  Sqlite3_INCLUDE_DIRS - the Sqlite3 include directories
#  Sqlite3_LIBRARIES - link these to use Sqlite3

FIND_PATH(Sqlite3_INCLUDE_DIR sqlite3.h
  /usr/include
  /usr/local/include
  /opt/local/include
)

FIND_LIBRARY(Sqlite3_LIBRARY
  NAMES ${Sqlite3_NAMES} libsqlite3.so libsqlite3.dylib
  PATHS /usr/lib /usr/local/lib /opt/local/lib
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Sqlite3 DEFAULT_MSG Sqlite3_LIBRARY Sqlite3_INCLUDE_DIR)

IF(Sqlite3_FOUND)
  SET(Sqlite3_LIBRARIES ${Sqlite3_LIBRARY})
  SET(Sqlite3_INCLUDE_DIRS ${Sqlite3_INCLUDE_DIR})
ENDIF(Sqlite3_FOUND)
