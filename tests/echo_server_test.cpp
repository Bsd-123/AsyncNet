#include "asyncnet/echo_server.hpp"

#include <arpa/inet.h>
#include <csignal>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <optional>
#include <string>
#include <thread>
#include <vector>

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

bool writeExactly(int fd, const std::string& data) {
    std::size_t total = 0;
    while (total < data.size()) {
        const ssize_t n = write(fd, data.data() + total, data.size() - total);
        if (n <= 0) return false;
        total += static_cast<std::size_t>(n);
    }
    return true;
}

bool readExactlyInto(int fd, std::string& out) {
    std::size_t total = 0;
    while (total < out.size()) {
        const ssize_t n = read(fd, out.data() + total, out.size() - total);
        if (n <= 0) return false;
        total += static_cast<std::size_t>(n);
    }
    return true;
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

TEST(EchoServer, ShutdownStopsAcceptingNewConnectionsImmediately) {
    using namespace std::chrono_literals;
    asyncnet::EchoServer server(0, std::nullopt, 50ms);
    const std::uint16_t port = server.boundPort();

    server.requestShutdown(); // closes the listen socket synchronously

    const int clientFd = connectLoopbackBlocking(port);
    EXPECT_LT(clientFd, 0); // nothing listening anymore: connection refused
    if (clientFd >= 0) close(clientFd);
}

TEST(EchoServer, GracefulShutdownFlushesPendingEchoBeforeClosing) {
    using namespace std::chrono_literals;
    asyncnet::EchoServer server(0, std::nullopt, 200ms); // generous grace period

    const int clientFd = connectLoopbackBlocking(server.boundPort());
    ASSERT_GE(clientFd, 0);
    server.runOnce(); // accept

    ASSERT_EQ(write(clientFd, "bye", 3), 3);
    server.runOnce(); // read + echo into the write buffer (not flushed yet)

    server.requestShutdown();

    // Even mid-shutdown, the already-queued echo must still go out rather
    // than being discarded -- graceful means give in-flight writes a
    // chance, not "drop everything immediately".
    for (int i = 0; i < 50 && !server.loop().stopRequested(); ++i) {
        server.runOnce();
    }
    EXPECT_TRUE(server.loop().stopRequested());

    char buf[3] = {};
    std::size_t total = 0;
    while (total < 3) {
        const ssize_t n = read(clientFd, buf + total, 3 - total);
        ASSERT_GT(n, 0);
        total += static_cast<std::size_t>(n);
    }
    EXPECT_EQ(std::string(buf, 3), "bye");

    close(clientFd);
}

TEST(EchoServer, GracefulShutdownForceClosesStragglersWithinBoundedWindow) {
    using namespace std::chrono_literals;
    asyncnet::EchoServer server(0, std::nullopt, 60ms);

    // A connected-but-silent client: no data pending in either direction,
    // so nothing would ever make it close on its own. The bounded grace
    // period, not client behavior, is what has to end this.
    const int clientFd = connectLoopbackBlocking(server.boundPort());
    ASSERT_GE(clientFd, 0);
    server.runOnce(); // accept

    server.requestShutdown();

    for (int i = 0; i < 50 && !server.loop().stopRequested(); ++i) {
        server.runOnce();
    }
    EXPECT_TRUE(server.loop().stopRequested());

    char buf[1];
    EXPECT_EQ(recv(clientFd, buf, sizeof(buf), 0), 0); // force-closed, not hung

    close(clientFd);
}

TEST(EchoServer, SigintTriggersGracefulShutdownEndToEnd) {
    using namespace std::chrono_literals;
    asyncnet::EchoServer server(0, std::nullopt, 60ms);

    raise(SIGINT); // handler writes to the shutdown eventfd synchronously

    for (int i = 0; i < 50 && !server.loop().stopRequested(); ++i) {
        server.runOnce();
    }
    EXPECT_TRUE(server.loop().stopRequested());
}

// Correctness under concurrency, not a performance/throughput claim (that's
// M3's job): 100+ real loopback TCP clients hammering a single-threaded
// EchoServer at once, each verifying its own bytes come back unmangled and
// undivided between connections.
TEST(EchoServer, HandlesOneHundredConcurrentClientsCorrectly) {
    using namespace std::chrono_literals;
    // Generous idle timeout used purely as a safety net bounding how long
    // any single runOnce() can block -- real clients finish in well under a
    // second, this just keeps a genuine bug from hanging the test forever.
    asyncnet::EchoServer server(0, std::optional(5000ms));
    const std::uint16_t port = server.boundPort();

    constexpr int kNumClients = 120;
    constexpr std::size_t kMessageSize = 256;

    std::vector<std::thread> clients;
    // Each thread writes only its own index -- no data race, and unlike
    // vector<bool> this isn't bit-packed, so no false sharing between
    // concurrent writes to neighboring entries.
    std::vector<char> ok(kNumClients, 0);
    std::atomic<int> doneCount{0};

    clients.reserve(kNumClients);
    for (int i = 0; i < kNumClients; ++i) {
        clients.emplace_back([i, port, &ok, &doneCount]() {
            struct DoneGuard {
                std::atomic<int>& counter;
                ~DoneGuard() { ++counter; }
            } doneGuard{doneCount};

            const int fd = connectLoopbackBlocking(port);
            if (fd < 0) return;

            std::string sent(kMessageSize, '\0');
            for (std::size_t j = 0; j < kMessageSize; ++j) {
                sent[j] = static_cast<char>((i * 31 + static_cast<int>(j)) % 256);
            }

            std::string received(kMessageSize, '\0');
            const bool roundTripOk =
                writeExactly(fd, sent) && readExactlyInto(fd, received) && received == sent;

            close(fd);
            ok[i] = roundTripOk ? 1 : 0;
        });
    }

    const auto deadline = std::chrono::steady_clock::now() + 10s;
    while (doneCount.load() < kNumClients && std::chrono::steady_clock::now() < deadline) {
        server.runOnce();
    }

    for (auto& t : clients) t.join();

    ASSERT_EQ(doneCount.load(), kNumClients) << "test timed out before all clients finished";
    for (int i = 0; i < kNumClients; ++i) {
        EXPECT_EQ(ok[i], 1) << "client " << i << " did not receive a correct echo";
    }
}
