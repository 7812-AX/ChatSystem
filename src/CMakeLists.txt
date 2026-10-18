# ChatSystem 构建配置
#
# 依赖：
#   - C++17 编译器
#   - libmysqlclient（MySQL C API）
#   - nlohmann/json（消息历史 JSON 响应）
#   - pthread

option(CHAT_TEST_HOOKS "启用确定性并发测试钩子（不影响正常业务语义）" OFF)

find_package(Threads REQUIRED)

# ==================== MySQL C API ====================

find_path(MYSQL_INCLUDE_DIR
    NAMES mysql/mysql.h
    HINTS
        /usr/include
        /usr/local/include
        /usr/local/mysql/include
        /opt/homebrew/include
)

find_library(MYSQL_LIBRARY
    NAMES mysqlclient mariadb
    HINTS
        /usr/lib
        /usr/local/lib
        /usr/local/mysql/lib
        /opt/homebrew/lib
)

if(NOT MYSQL_INCLUDE_DIR OR NOT MYSQL_LIBRARY)
    message(FATAL_ERROR
        "未找到 MySQL 客户端库。请安装 libmysqlclient-dev（Debian/Ubuntu）"
        "或 mysql-client（macOS/Homebrew），并确认 mysql.h 可被找到。")
endif()

# ==================== nlohmann/json ====================

find_package(nlohmann_json QUIET)

if(NOT nlohmann_json_FOUND)
    find_path(NLOHMANN_JSON_INCLUDE_DIR nlohmann/json.hpp)
    if(NOT NLOHMANN_JSON_INCLUDE_DIR)
        message(FATAL_ERROR
            "未找到 nlohmann/json.hpp。请安装 nlohmann-json3-dev（Debian/Ubuntu）"
            "或 nlohmann-json（macOS/Homebrew）。")
    endif()
endif()

# ==================== 源文件 ====================

set(SOURCES
    # 公共层
    common/thread_pool.cpp
    common/logger.cpp

    # 数据层
    database/user_dao.cpp
    database/friend_dao.cpp
    database/message_dao.cpp
    database/mysql_connect_pool.cpp

    # 网络层
    network/socket_utils.cpp
    network/connection.cpp
    network/tcp_connection.cpp
    network/event_loop.cpp

    # 业务层
    service/auth_service.cpp
    service/message_service.cpp

    # 会话层
    session/session_store.cpp
    session/user_id_cache.cpp
)

# ==================== 服务端 ====================

add_executable(chat_server ${SOURCES} main.cpp)

target_include_directories(chat_server PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${MYSQL_INCLUDE_DIR}
)

if(nlohmann_json_FOUND)
    target_link_libraries(chat_server PRIVATE nlohmann_json::nlohmann_json)
else()
    target_include_directories(chat_server PRIVATE ${NLOHMANN_JSON_INCLUDE_DIR})
endif()

target_link_libraries(chat_server PRIVATE
    ${MYSQL_LIBRARY}
    Threads::Threads
)

target_compile_options(chat_server PRIVATE -Wall -Wextra -pedantic)

if(CHAT_TEST_HOOKS)
    target_sources(chat_server PRIVATE common/test_hooks.cpp)
    target_compile_definitions(chat_server PRIVATE CHAT_TEST_HOOKS)
endif()

# ==================== 负载驱动器 ====================

add_executable(chat_loadgen tools/chat_loadgen.cpp)

target_compile_options(chat_loadgen PRIVATE -Wall -Wextra -pedantic)
target_link_libraries(chat_loadgen PRIVATE Threads::Threads)

# ==================== 构建摘要 ====================

message(STATUS "MySQL include: ${MYSQL_INCLUDE_DIR}")
message(STATUS "MySQL library: ${MYSQL_LIBRARY}")
