cmake_minimum_required(VERSION 3.1)

project(kv_fdw)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_C_STANDARD 99)

#init pg enviroment
execute_process(COMMAND pg_config --includedir-server OUTPUT_VARIABLE PG_INCLUDE OUTPUT_STRIP_TRAILING_WHITESPACE)
execute_process(COMMAND pg_config --sharedir OUTPUT_VARIABLE PG_SHARE_DIR OUTPUT_STRIP_TRAILING_WHITESPACE)
execute_process(COMMAND pg_config --libdir OUTPUT_VARIABLE PG_LIB_DIR OUTPUT_STRIP_TRAILING_WHITESPACE)

include_directories(${PG_INCLUDE})
include_directories(${PROJECT_NAME} 
    ./src/
    )

add_library(${PROJECT_NAME}   SHARED
    src/server/kv_storage.cc
    src/server/kv_manager.cc
    src/server/kv_worker.cc
    src/server/kv_storage.cc
    src/kv_fdw.c
    src/kv_utility.c
    src/ipc/kv_posix.cc
    src/ipc/kv_message.cc
    src/ipc/kv_channel.cc
    src/ipc/kv_mq.cc
    src/client/kv_client.cc
    )

target_link_directories(${PROJECT_NAME} PRIVATE ${PG_LIB_DIR})
target_link_libraries(${PROJECT_NAME}  protobuf-c ecpg pthread rocksdb)

# cancel prefix "lib"
SET_TARGET_PROPERTIES(${PROJECT_NAME}  PROPERTIES PREFIX "") 

install(TARGETS ${PROJECT_NAME}  DESTINATION ${PG_LIB_DIR})
install(FILES kv_fdw.control kv_fdw--0.0.1.sql DESTINATION ${PG_SHARE_DIR}/extension)
