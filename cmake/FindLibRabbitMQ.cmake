# - Find librabbitmq
# Find the native RABBITMQ headers and libraries.
#
#  RABBITMQ_INCLUDE_DIRS - where to find curl/curl.h, etc.
#  RABBITMQ_LIBRARIES    - List of libraries when using curl.
#  RABBITMQ_FOUND        - True if curl found.

# Look for the header file.
FIND_PATH(RABBITMQ_INCLUDE_DIR NAMES amqp.h amqp_framing.h)
MARK_AS_ADVANCED(RABBITMQ_INCLUDE_DIR)

# Look for the library.
FIND_LIBRARY(RABBITMQ_LIBRARY NAMES 
    rabbitmq
)
MARK_AS_ADVANCED(RABBITMQ_LIBRARY)

# handle the QUIETLY and REQUIRED arguments and set RABBITMQ_FOUND to TRUE if 
# all listed variables are TRUE
INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(RABBITMQ DEFAULT_MSG RABBITMQ_LIBRARY RABBITMQ_INCLUDE_DIR)

IF(RABBITMQ_FOUND)
  SET(RABBITMQ_LIBRARIES ${RABBITMQ_LIBRARY})
  SET(RABBITMQ_INCLUDE_DIRS ${RABBITMQ_INCLUDE_DIR})
ENDIF(RABBITMQ_FOUND)
