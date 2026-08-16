find_package(GTest CONFIG REQUIRED)
include(GoogleTest)

add_executable(asyncnet_tests
    logger_test.cpp
)
target_link_libraries(asyncnet_tests PRIVATE
    asyncnet_core
    GTest::gtest
    GTest::gtest_main
)

gtest_discover_tests(asyncnet_tests)
