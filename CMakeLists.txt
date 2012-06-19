cmake_minimum_required (VERSION 2.6)
project (u1db)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/")

include_directories(include)
add_subdirectory(src)

find_package(CURL)
if (CURL_FOUND)
  include_directories(${CURL_INCLUDE_DIRS})
endif (CURL_FOUND)

find_package(OAUTH)
if (OAUTH_FOUND)
  include_directories(${OAUTH_INCLUDE_DIRS})
endif (OAUTH_FOUND)

find_package(JSON)
if (JSON_FOUND)
  include_directories(${JSON_INCLUDE_DIRS})
endif (JSON_FOUND)


