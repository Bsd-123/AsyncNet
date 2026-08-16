#include "asyncnet/echo_server.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <optional>
#include <string>
#include <thread>

#include <gtest/gtest.h>

namespace {

int connectLoopbackBlocking(std::uint16_t port) {
    const int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    if (connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        close(fd);
        return -1;
    }
    return fd;
}

} // namespace

TEST(EchoServer, EchoesDataOverRealTcpConnection) {
    asyncnet::EchoServer server(0);

    const int clientFd = connectLoopbackBlocking(server.boundPort());
    ASSERT_GE(clientFd, 0);

    server.runOnce(); // accept

    const std::string msg = "round trip";
    ASSERT_EQ(write(clientFd, msg.data(), msg.size()), static_cast<ssize_t>(msg.size()));

    server.runOnce(); // read + echo into write buffer
    server.runOnce(); // flush the echoed bytes back out

    std::string received(msg.size(), '\0');
    std::size_t total = 0;
    while (total < msg.size()) {
        const ssize_t n = read(clientFd, received.data() + total, msg.size() - total);
        ASSERT_GT(n, 0);
        total += static_cast<std::size_t>(n);
    }
    EXPECT_EQ(received, msg);

    close(clientFd);
}

TEST(EchoServer, IdleConnectionIsClosedAfterTimeoutEndToEnd) {
    using namespace std::chrono_literals;
    asyncnet::EchoServer server(0, std::optional(80ms));

    const int clientFd = connectLoopbackBlocking(server.boundPort());
    ASSERT_GE(clientFd, 0);

    server.runOnce(); // accept -> starts the idle clock for this connection

    // Send nothing. Once real time has passed the configured timeout,
    // checkExpired() (folded into runOnce()) must close it on its own --
    // no per-connection timerfd, just the idle-timeout sweep.
    std::this_thread::sleep_for(100ms);
    server.runOnce(); // poll(~0ms, already due) -> detects expiry, queues close
    server.runOnce(); // flushes the deferred close from the cycle above

    char buf[1];
    const ssize_t n = recv(clientFd, buf, sizeof(buf), 0);
    EXPECT_EQ(n, 0); // EOF: server closed its end due to idle timeout

    close(clientFd);
}

TEST(EchoServer, ActivityResetsIdleTimeoutEndToEnd) {
    using namespace std::chrono_literals;
    asyncnet::EchoServer server(0, std::optional(80ms));

    const int clientFd = connectLoopbackBlocking(server.boundPort());
    ASSERT_GE(clientFd, 0);

    server.runOnce(); // accept

    // Stay active across what would otherwise be the timeout window.
    for (int i = 0; i < 3; ++i) {
        std::this_thread::sleep_for(40ms);
        ASSERT_EQ(write(clientFd, "hi", 2), 2);
        server.runOnce(); // read + echo (refreshes activity)
        server.runOnce(); // flush the echo back out

        char buf[2];
        std::size_t total = 0;
        while (total < 2) {
            const ssize_t n = read(clientFd, buf + total, 2 - total);
            ASSERT_GT(n, 0);
            total += static_cast<std::size_t>(n);
        }
    }

    // Still alive: a further idle-only wait shorter than the timeout must
    // not have closed it.
    std::this_thread::sleep_for(30ms);
    server.runOnce();

    ASSERT_EQ(write(clientFd, "!", 1), 1);
    server.runOnce();
    server.runOnce();
    char last = 0;
    EXPECT_EQ(read(clientFd, &last, 1), 1);
    EXPECT_EQ(last, '!');

    close(clientFd);
}
