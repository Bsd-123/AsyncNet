#include "asyncnet/echo_server.hpp"

#include <algorithm>
#include <cstdio>
#include <string>

#include "asyncnet/logger.hpp"

namespace asyncnet {

EchoServer::EchoServer(std::uint16_t port,
                        std::optional<std::chrono::milliseconds> idleTimeout,
                        std::chrono::milliseconds shutdownGracePeriod)
    : shutdownSignal_(loop_, [this]() { requestShutdown(); }),
      shutdownGracePeriod_(shutdownGracePeriod) {
    acceptor_.emplace(loop_, port, [this](int fd) { onNewConnection(fd); });
    if (idleTimeout) {
        idleTimeout_.emplace(*idleTimeout);
    }
}

void EchoServer::run() {
    while (!loop_.stopRequested()) {
        runOnce();
    }
}

void EchoServer::runOnce() {
    int timeoutMs = idleTimeout_ ? idleTimeout_->nextPollTimeoutMs() : -1;

    if (shuttingDown_) {
        const auto remaining = shutdownDeadline_ - Clock::now();
        // Round UP to milliseconds, not truncate: a sub-millisecond
        // positive remainder truncating to 0 would make this a
        // non-blocking poll() that returns instantly without waiting out
        // the gap, and since real time barely advances per iteration at
        // that point, the loop would busy-spin for many cycles instead of
        // just blocking the remaining sliver of a millisecond.
        const int shutdownMs =
            remaining > Clock::duration::zero()
                ? static_cast<int>(
                      std::chrono::ceil<std::chrono::milliseconds>(remaining).count())
                : 0;
        timeoutMs = (timeoutMs < 0) ? shutdownMs : std::min(timeoutMs, shutdownMs);
    }

    loop_.runOnce(timeoutMs);

    if (idleTimeout_) {
        idleTimeout_->checkExpired([this](int fd) {
            ASYNCNET_LOG_INFO("connection fd=" + std::to_string(fd) +
                               ": idle timeout, closing");
            const auto it = connections_.find(fd);
            if (it != connections_.end()) {
                it->second->closeIdle();
            }
        });
    }

    if (shuttingDown_) {
        if (connections_.empty()) {
            loop_.stop();
        } else if (Clock::now() >= shutdownDeadline_) {
            ASYNCNET_LOG_INFO("shutdown grace period elapsed, force-closing " +
                               std::to_string(connections_.size()) +
                               " remaining connection(s)");
            forceCloseRemainingConnections();
            // Not stopping yet: the close actions just queued need the
            // *next* loop_.runOnce() to actually flush (close the fds,
            // erase from connections_) before it's safe to exit the loop.
        }
    }
}

void EchoServer::requestShutdown() {
    if (shuttingDown_) {
        return;
    }
    shuttingDown_ = true;
    shutdownDeadline_ = Clock::now() + shutdownGracePeriod_;

    ASYNCNET_LOG_INFO("shutdown requested: no longer accepting new connections, "
                       "flushing up to " +
                       std::to_string(shutdownGracePeriod_.count()) + "ms");

    // Stop accepting immediately (M1 plan step 9): destroying the Acceptor
    // closes and deregisters the listen socket right away.
    acceptor_.reset();
}

void EchoServer::forceCloseRemainingConnections() {
    for (auto& [fd, connection] : connections_) {
        connection->closeForShutdown();
    }
}

void EchoServer::onNewConnection(int fd) {
    connections_[fd] = std::make_unique<Connection>(
        loop_, fd, [this](int closedFd) { onConnectionClosed(closedFd); },
        Connection::kDefaultBufferCapacity,
        [this](int activeFd) { onConnectionActivity(activeFd); });

    if (idleTimeout_) {
        idleTimeout_->recordActivity(fd);
    }
}

void EchoServer::onConnectionClosed(int fd) {
    if (idleTimeout_) {
        idleTimeout_->remove(fd);
    }
    connections_.erase(fd);
}

void EchoServer::onConnectionActivity(int fd) {
    if (idleTimeout_) {
        idleTimeout_->recordActivity(fd);
    }
}

} // namespace asyncnet
