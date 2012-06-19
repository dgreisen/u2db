cmake_minimum_required (VERSION 2.6)
project (u1db)
add_library (u1db STATIC SHARED mkstemp_compat.c u1db.c u1db_http_sync_target.c u1db_query.c  u1db_sync_target.c  u1db_uuid.c  u1db_vectorclock.c)
