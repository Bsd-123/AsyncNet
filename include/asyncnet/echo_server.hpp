#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <unordered_map>

#include "asyncnet/acceptor.hpp"
#include "asyncnet/connection.hpp"
#include "asyncnet/event_loop.hpp"
#include "asyncnet/idle_timeout_manager.hpp"

namespace asyncnet {

// Ties EventLoop, Acceptor, Connection, and idle-timeout detection together
// into a runnable TCP echo server -- the M1 deliverable.
class EchoServer {
public:
    explicit EchoServer(std::uint16_t port,
                         std::optional<std::chrono::milliseconds> idleTimeout = std::nullopt);

    // Runs until the underlying EventLoop is stopped.
    void run();

    // One poll cycle plus an idle-timeout sweep. run() is just this in a
    // loop; exposed directly so tests can drive bounded, deterministic
    // cycles instead of a blocking run().
    void runOnce();

    std::uint16_t boundPort() const { return acceptor_.boundPort(); }
    EventLoop& loop() { return loop_; }

private:
    void onNewConnection(int fd);
    void onConnectionClosed(int fd);
    void onConnectionActivity(int fd);

    EventLoop loop_;
    Acceptor acceptor_;
    std::optional<IdleTimeoutManager> idleTimeout_;
    std::unordered_map<int, std::unique_ptr<Connection>> connections_;
};

} // namespace asyncnet
