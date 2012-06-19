cmake_minimum_required (VERSION 2.6)
project (u1db)

add_custom_command (
  OUTPUT ${CMAKE_SOURCE_DIR}/src/u1db_schema.c
  COMMAND python ${CMAKE_SOURCE_DIR}/sql_to_c.py ${CMAKE_SOURCE_DIR}/u1db/backends/dbschema.sql u1db__schema ${CMAKE_SOURCE_DIR}/src/u1db_schema.c
  DEPENDS ${CMAKE_SOURCE_DIR}/u1db/backends/dbschema.sql ${CMAKE_SOURCE_DIR}/sql_to_c.py
  )

add_library (u1db STATIC SHARED mkstemp_compat.c u1db.c u1db_http_sync_target.c u1db_query.c  u1db_sync_target.c  u1db_uuid.c  u1db_vectorclock.c u1db_schema.c)
