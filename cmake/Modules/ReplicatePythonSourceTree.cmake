# Note: when executed in the build dir, then CMAKE_CURRENT_SOURCE_DIR is the
# build dir.
file( COPY setup.py u1db  DESTINATION "${CMAKE_ARGV3}"
  FILES_MATCHING PATTERN "*.py" )
file( COPY setup.py u1db  DESTINATION "${CMAKE_ARGV3}"
  FILES_MATCHING PATTERN "*.pyx" )
file( COPY setup.py u1db  DESTINATION "${CMAKE_ARGV3}"
  FILES_MATCHING PATTERN "*.sql" )
file( COPY setup.py u1db  DESTINATION "${CMAKE_ARGV3}"
  FILES_MATCHING PATTERN "*.key" )
file( COPY setup.py u1db  DESTINATION "${CMAKE_ARGV3}"
  FILES_MATCHING PATTERN "*.cert" )
