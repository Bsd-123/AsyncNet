#pragma once

#include <cstdio>
#include <ctime>
#include <chrono>
#include <string_view>

namespace asyncnet {

enum class LogLevel { Debug, Info, Warn, Error };

namespace detail {

inline LogLevel& minLevel() {
    static LogLevel level = LogLevel::Info;
    return level;
}

inline const char* levelName(LogLevel level) {
    switch (level) {
        case LogLevel::Debug: return "DEBUG";
        case LogLevel::Info:  return "INFO";
        case LogLevel::Warn:  return "WARN";
        case LogLevel::Error: return "ERROR";
    }
    return "UNKNOWN";
}

} // namespace detail

inline void setMinLogLevel(LogLevel level) {
    detail::minLevel() = level;
}

// Minimal leveled, timestamped logger. Callers format their own messages
// (e.g. with std::format) before passing them in - this stays a sink, not a
// formatting engine.
inline void log(LogLevel level, std::string_view message) {
    if (level < detail::minLevel()) {
        return;
    }

    const auto now = std::chrono::system_clock::now();
    const std::time_t nowTimeT = std::chrono::system_clock::to_time_t(now);
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                        now.time_since_epoch()) % 1000;

    std::tm tmBuf{};
    localtime_r(&nowTimeT, &tmBuf);

    std::fprintf(stderr, "%04d-%02d-%02d %02d:%02d:%02d.%03lld [%s] %.*s\n",
                 tmBuf.tm_year + 1900, tmBuf.tm_mon + 1, tmBuf.tm_mday,
                 tmBuf.tm_hour, tmBuf.tm_min, tmBuf.tm_sec,
                 static_cast<long long>(ms.count()), detail::levelName(level),
                 static_cast<int>(message.size()), message.data());
}

} // namespace asyncnet

#define ASYNCNET_LOG_DEBUG(msg) ::asyncnet::log(::asyncnet::LogLevel::Debug, (msg))
#define ASYNCNET_LOG_INFO(msg)  ::asyncnet::log(::asyncnet::LogLevel::Info,  (msg))
#define ASYNCNET_LOG_WARN(msg)  ::asyncnet::log(::asyncnet::LogLevel::Warn,  (msg))
#define ASYNCNET_LOG_ERROR(msg) ::asyncnet::log(::asyncnet::LogLevel::Error, (msg))
