INCLUDE_DIRECTORIES(
        ${PROJECT_SOURCE_DIR}/postgres/include
        ${PROJECT_SOURCE_DIR}/runtime/include
)

LOCATE_LIBRARY(libpqxx "pqxx/version.hxx" "pqxx")

ADD_LIBRARY(runtime MODULE
        src/service/runtime.cpp
        src/module.cpp
        ../postgres/src/pool.cpp
        )

TARGET_LINK_LIBRARIES(runtime
        metrics
        msgpack
        blackhole
        cocaine-core
        cocaine-io-util
        pqxx)

SET_TARGET_PROPERTIES(runtime PROPERTIES
        PREFIX ""
        SUFFIX "${COCAINE_PLUGIN_SUFFIX}"
        COMPILE_FLAGS "-std=c++0x -Wall -Werror -Wextra -pedantic -Winit-self -Wold-style-cast -Woverloaded-virtual -Wshadow -Wctor-dtor-privacy -Wnon-virtual-dtor")

INSTALL(TARGETS runtime
        LIBRARY DESTINATION lib/cocaine
        COMPONENT runtime)

