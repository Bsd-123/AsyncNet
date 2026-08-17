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
#include "asyncnet/shutdown_signal.hpp"

namespace asyncnet {

// Ties EventLoop, Acceptor, Connection, idle-timeout detection, and
// SIGINT/SIGTERM-driven graceful shutdown together into a runnable TCP
// echo server -- the M1 deliverable.
class EchoServer {
public:
    explicit EchoServer(std::uint16_t port,
                         std::optional<std::chrono::milliseconds> idleTimeout = std::nullopt,
                         std::chrono::milliseconds shutdownGracePeriod = std::chrono::seconds(5));

    // Runs until shutdown completes (or the loop is otherwise stopped).
    void run();

    // One poll cycle, plus idle-timeout and shutdown-deadline sweeps.
    // run() is just this in a loop; exposed directly so tests can drive
    // bounded, deterministic cycles instead of a blocking run().
    void runOnce();

    // Stops accepting immediately and starts the bounded grace-period
    // flush-then-close sequence. Also what SIGINT/SIGTERM trigger; exposed
    // directly so tests (and callers embedding the server) don't need to
    // send a real OS signal to exercise the same path.
    void requestShutdown();

    std::uint16_t boundPort() const { return acceptor_->boundPort(); }
    EventLoop& loop() { return loop_; }

private:
    void onNewConnection(int fd);
    void onConnectionClosed(int fd);
    void onConnectionActivity(int fd);
    void forceCloseRemainingConnections();

    using Clock = std::chrono::steady_clock;

    EventLoop loop_;
    std::optional<Acceptor> acceptor_;
    std::optional<IdleTimeoutManager> idleTimeout_;
    std::unordered_map<int, std::unique_ptr<Connection>> connections_;
    ShutdownSignal shutdownSignal_;

    std::chrono::milliseconds shutdownGracePeriod_;
    bool shuttingDown_ = false;
    Clock::time_point shutdownDeadline_{}; // valid only once shuttingDown_ is true
};

} // namespace asyncnet
