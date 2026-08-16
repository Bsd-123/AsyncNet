find_package(GTest CONFIG REQUIRED)
find_package(Threads REQUIRED)
include(GoogleTest)

add_executable(asyncnet_tests
    logger_test.cpp
    epoll_reactor_test.cpp
    event_loop_test.cpp
)
target_include_directories(asyncnet_tests PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
)
target_link_libraries(asyncnet_tests PRIVATE
    asyncnet_core
    GTest::gtest
    GTest::gtest_main
    Threads::Threads
)

gtest_discover_tests(asyncnet_tests)
