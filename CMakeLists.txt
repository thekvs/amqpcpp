PROJECT(amqpcpp)

CMAKE_MINIMUM_REQUIRED(VERSION 2.8 FATAL_ERROR)

SET(CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake/)

OPTION(DEBUG_MODE          "Enable debug output [default: ON]" ON)
OPTION(ENABLE_OPTIMIZATION "Enable optimization [default: OFF]"OFF)

INCLUDE(CheckTypeSize)
INCLUDE(CheckIncludeFile)
INCLUDE(CheckCXXCompilerFlag)

FIND_PACKAGE(LibRabbitMQ)

INCLUDE_DIRECTORIES(${amqpcpp_SOURCE_DIR}/)
INCLUDE_DIRECTORIES(${amqpcpp_SOURCE_DIR}/include/)

INCLUDE_DIRECTORIES(${RABBITMQ_INCLUDE_DIRS})

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -W")

IF(DEBUG_MODE MATCHES "ON")
    SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g")
ENDIF(DEBUG_MODE MATCHES "ON")

IF(ENABLE_OPTIMIZATION MATCHES "ON")
    SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3")
ELSE(ENABLE_OPTIMIZATION MATCHES "ON")
    SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0")
ENDIF(ENABLE_OPTIMIZATION MATCHES "ON")

SET(SRCS        ${amqpcpp_SOURCE_DIR}/src/AMQPBase.cpp
		${amqpcpp_SOURCE_DIR}/src/AMQP.cpp
		${amqpcpp_SOURCE_DIR}/src/AMQPException.cpp
		${amqpcpp_SOURCE_DIR}/src/AMQPExchange.cpp
		${amqpcpp_SOURCE_DIR}/src/AMQPMessage.cpp
		${amqpcpp_SOURCE_DIR}/src/AMQPQueue.cpp
)

ADD_LIBRARY(libamqpcpp-static STATIC ${SRCS})
SET_TARGET_PROPERTIES(libamqpcpp-static PROPERTIES OUTPUT_NAME amqpcpp)
SET_TARGET_PROPERTIES(libamqpcpp-static PROPERTIES LINKER_LANGUAGE CXX)

SET(LINKLIBS libamqpcpp-static ${RABBITMQ_LIBRARIES})

ADD_EXECUTABLE(example_consume ${amqpcpp_SOURCE_DIR}/examples/example_consume.cpp)
TARGET_LINK_LIBRARIES(example_consume ${LINKLIBS})

ADD_EXECUTABLE(example_get ${amqpcpp_SOURCE_DIR}/examples/example_get.cpp)
TARGET_LINK_LIBRARIES(example_get ${LINKLIBS})

ADD_EXECUTABLE(example_publish ${amqpcpp_SOURCE_DIR}/examples/example_publish.cpp)
TARGET_LINK_LIBRARIES(example_publish ${LINKLIBS})
