#include "asyncnet/acceptor.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <string>

#include "asyncnet/logger.hpp"

namespace asyncnet {

namespace {

constexpr int kListenBacklog = SOMAXCONN;

[[noreturn]] void throwErrno(const char* what) {
    throw std::runtime_error(std::string(what) + " failed: " + std::strerror(errno));
}

} // namespace

Acceptor::Acceptor(EventLoop& loop, std::uint16_t port,
                    NewConnectionHandler onNewConnection)
    : loop_(loop), listenFd_(-1), onNewConnection_(std::move(onNewConnection)) {
    listenFd_ = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (listenFd_ < 0) {
        throwErrno("socket");
    }

    const int reuse = 1;
    if (setsockopt(listenFd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) != 0) {
        close(listenFd_);
        throwErrno("setsockopt(SO_REUSEADDR)");
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);

    if (bind(listenFd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        close(listenFd_);
        throwErrno("bind");
    }

    if (listen(listenFd_, kListenBacklog) != 0) {
        close(listenFd_);
        throwErrno("listen");
    }

    loop_.reactor().registerFd(
        listenFd_, IOEvent::Readable,
        [this](int fd, IOEvent events) { onReadable(fd, events); });
}

Acceptor::~Acceptor() {
    if (listenFd_ >= 0) {
        loop_.reactor().removeFd(listenFd_);
        close(listenFd_);
    }
}

std::uint16_t Acceptor::boundPort() const {
    sockaddr_in addr{};
    socklen_t len = sizeof(addr);
    if (getsockname(listenFd_, reinterpret_cast<sockaddr*>(&addr), &len) != 0) {
        throwErrno("getsockname");
    }
    return ntohs(addr.sin_port);
}

void Acceptor::onReadable(int /*fd*/, IOEvent /*events*/) {
    for (;;) {
        const int connFd =
            accept4(listenFd_, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (connFd >= 0) {
            if (onNewConnection_) {
                onNewConnection_(connFd);
            }
            continue; // drain every pending connection from this wakeup
        }

        switch (errno) {
            case EAGAIN: // == EWOULDBLOCK on Linux
                return;  // backlog fully drained

            case EINTR:
                continue; // transient, retry immediately

            case ECONNABORTED:
                ASYNCNET_LOG_WARN(
                    "accept4: connection aborted before accept, continuing");
                continue; // transient, keep draining the rest of the backlog

            case EMFILE:
            case ENFILE:
                ASYNCNET_LOG_ERROR(
                    "accept4: fd exhaustion, pausing accept until next wakeup");
                // Do not busy-loop retrying the same failure. Level-triggered
                // epoll keeps reporting this fd readable while the backlog
                // is non-empty, so accepting resumes on a later wakeup once
                // fds free up -- that's the documented policy (M1 plan §4).
                return;

            default:
                ASYNCNET_LOG_ERROR(std::string("accept4: unexpected error: ") +
                                    std::strerror(errno));
                // Unknown/non-transient error: stop for this wakeup rather
                // than risk a tight retry loop; try again next wakeup.
                return;
        }
    }
}

} // namespace asyncnet
