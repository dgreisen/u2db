# Note: when executed in the build dir, then CMAKE_CURRENT_SOURCE_DIR is the
# build dir.
file( COPY html-docs DESTINATION "${CMAKE_ARGV3}")
file( COPY setup.py u1db  DESTINATION "${CMAKE_ARGV3}"
  FILES_MATCHING
  PATTERN "*.py"
  PATTERN "*.pyx"
  PATTERN "*.pyx"
  PATTERN "*.sql"
  PATTERN "*.key"
  PATTERN "*.cert"
  PATTERN "*.pem" )
file( COPY "u1db-serve" "u1db-client" "MANIFEST.in" "README"
    "COPYING" "COPYING.LESSER" DESTINATION "${CMAKE_ARGV3}")
