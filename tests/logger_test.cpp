#include <gtest/gtest.h>

#include "asyncnet/logger.hpp"

TEST(Logger, LevelOrdering) {
    EXPECT_LT(asyncnet::LogLevel::Debug, asyncnet::LogLevel::Info);
    EXPECT_LT(asyncnet::LogLevel::Info, asyncnet::LogLevel::Warn);
    EXPECT_LT(asyncnet::LogLevel::Warn, asyncnet::LogLevel::Error);
}

TEST(Logger, EmitDoesNotCrashAtAnyLevel) {
    asyncnet::setMinLogLevel(asyncnet::LogLevel::Debug);
    EXPECT_NO_THROW(asyncnet::log(asyncnet::LogLevel::Debug, "debug message"));
    EXPECT_NO_THROW(asyncnet::log(asyncnet::LogLevel::Info, "info message"));
    EXPECT_NO_THROW(asyncnet::log(asyncnet::LogLevel::Warn, "warn message"));
    EXPECT_NO_THROW(asyncnet::log(asyncnet::LogLevel::Error, "error message"));
    asyncnet::setMinLogLevel(asyncnet::LogLevel::Info);
}

TEST(Logger, BelowMinLevelIsSuppressedWithoutCrashing) {
    asyncnet::setMinLogLevel(asyncnet::LogLevel::Error);
    EXPECT_NO_THROW(asyncnet::log(asyncnet::LogLevel::Debug, "should be suppressed"));
    asyncnet::setMinLogLevel(asyncnet::LogLevel::Info);
}
