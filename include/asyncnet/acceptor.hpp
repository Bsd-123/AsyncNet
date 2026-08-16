#pragma once

#include <cstdint>
#include <functional>

#include "asyncnet/event_loop.hpp"

namespace asyncnet {

// Non-blocking TCP listen socket. Drains every pending connection from the
// backlog on each wakeup and hands each new fd to the caller-supplied
// handler -- it knows nothing about Connection/protocol concerns.
class Acceptor {
public:
    using NewConnectionHandler = std::function<void(int fd)>;

    // port == 0 lets the OS pick an ephemeral port; see boundPort().
    Acceptor(EventLoop& loop, std::uint16_t port, NewConnectionHandler onNewConnection);
    ~Acceptor();

    Acceptor(const Acceptor&) = delete;
    Acceptor& operator=(const Acceptor&) = delete;

    int listenFd() const { return listenFd_; }
    std::uint16_t boundPort() const;

private:
    void onReadable(int fd, IOEvent events);

    EventLoop& loop_;
    int listenFd_;
    NewConnectionHandler onNewConnection_;
};

} // namespace asyncnet
