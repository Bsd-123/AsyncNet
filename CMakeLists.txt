cmake_minimum_required(VERSION 3.20)
project(AsyncNet LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE Debug CACHE STRING "Build type" FORCE)
endif()

option(ASYNCNET_ENABLE_ASAN_UBSAN "Build with AddressSanitizer + UndefinedBehaviorSanitizer" OFF)
option(ASYNCNET_BUILD_TESTS "Build unit/integration tests" ON)

add_library(asyncnet_warnings INTERFACE)
target_compile_options(asyncnet_warnings INTERFACE
    -Wall -Wextra -Wpedantic
)

if(ASYNCNET_ENABLE_ASAN_UBSAN)
    add_library(asyncnet_sanitizers INTERFACE)
    target_compile_options(asyncnet_sanitizers INTERFACE
        -fsanitize=address,undefined -fno-omit-frame-pointer -g -O1
    )
    target_link_options(asyncnet_sanitizers INTERFACE
        -fsanitize=address,undefined
    )
else()
    add_library(asyncnet_sanitizers INTERFACE)
endif()

add_library(asyncnet_core
    src/epoll_reactor.cpp
    src/event_loop.cpp
    src/acceptor.cpp
    src/ring_buffer.cpp
    src/connection.cpp
    src/idle_timeout_manager.cpp
    src/shutdown_signal.cpp
    src/echo_server.cpp
)
target_include_directories(asyncnet_core PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)
target_link_libraries(asyncnet_core PUBLIC asyncnet_warnings asyncnet_sanitizers)

add_executable(asyncnet_server src/main.cpp)
target_link_libraries(asyncnet_server PRIVATE asyncnet_core)

if(ASYNCNET_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()
