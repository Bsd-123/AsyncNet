#pragma once

#include <sys/socket.h>
#include <unistd.h>

#include <stdexcept>

namespace asyncnet::test {

// A connected AF_UNIX SOCK_STREAM pair, closed automatically.
struct SocketPair {
    int readEnd = -1;
    int writeEnd = -1;

    SocketPair() {
        int fds[2];
        if (socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0) {
            throw std::runtime_error("socketpair() failed");
        }
        readEnd = fds[0];
        writeEnd = fds[1];
    }

    ~SocketPair() {
        if (readEnd >= 0) close(readEnd);
        if (writeEnd >= 0) close(writeEnd);
    }

    SocketPair(const SocketPair&) = delete;
    SocketPair& operator=(const SocketPair&) = delete;
};

} // namespace asyncnet::test
