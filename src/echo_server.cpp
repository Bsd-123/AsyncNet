#include "asyncnet/echo_server.hpp"

#include <string>

#include "asyncnet/logger.hpp"

namespace asyncnet {

EchoServer::EchoServer(std::uint16_t port,
                        std::optional<std::chrono::milliseconds> idleTimeout)
    : acceptor_(loop_, port, [this](int fd) { onNewConnection(fd); }) {
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
    const int timeoutMs = idleTimeout_ ? idleTimeout_->nextPollTimeoutMs() : -1;
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
