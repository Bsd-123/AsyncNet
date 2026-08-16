#include "asyncnet/epoll_reactor.hpp"

#include <sys/socket.h>
#include <unistd.h>
#include <csignal>
#include <cerrno>

#include <atomic>
#include <chrono>
#include <thread>

#include <gtest/gtest.h>

namespace {

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

} // namespace

TEST(EpollReactor, RegisterDetectsReadable) {
    asyncnet::EpollReactor reactor;
    SocketPair sp;

    int firedFd = -1;
    asyncnet::IOEvent firedEvents = asyncnet::IOEvent::None;

    reactor.registerFd(sp.readEnd, asyncnet::IOEvent::Readable,
                        [&](int fd, asyncnet::IOEvent events) {
                            firedFd = fd;
                            firedEvents = events;
                        });

    const char byte = 'x';
    ASSERT_EQ(write(sp.writeEnd, &byte, 1), 1);

    const int n = reactor.poll(1000);

    EXPECT_EQ(n, 1);
    EXPECT_EQ(firedFd, sp.readEnd);
    EXPECT_TRUE(asyncnet::hasEvent(firedEvents, asyncnet::IOEvent::Readable));
}

TEST(EpollReactor, ModifyChangesWatchedEvents) {
    asyncnet::EpollReactor reactor;
    SocketPair sp;

    int callCount = 0;
    asyncnet::IOEvent lastEvents = asyncnet::IOEvent::None;

    // A fresh socket's write end is immediately writable.
    reactor.registerFd(sp.writeEnd, asyncnet::IOEvent::Writable,
                        [&](int, asyncnet::IOEvent events) {
                            ++callCount;
                            lastEvents = events;
                        });

    ASSERT_EQ(reactor.poll(1000), 1);
    EXPECT_TRUE(asyncnet::hasEvent(lastEvents, asyncnet::IOEvent::Writable));

    // Switch to watching only Readable: no data pending, so poll() with a
    // short timeout should now report nothing for this fd.
    reactor.modifyFd(sp.writeEnd, asyncnet::IOEvent::Readable);
    callCount = 0;
    EXPECT_EQ(reactor.poll(100), 0);
    EXPECT_EQ(callCount, 0);
}

TEST(EpollReactor, RemoveFdStopsDelivery) {
    asyncnet::EpollReactor reactor;
    SocketPair sp;

    bool called = false;
    reactor.registerFd(sp.readEnd, asyncnet::IOEvent::Readable,
                        [&](int, asyncnet::IOEvent) { called = true; });

    reactor.removeFd(sp.readEnd);

    const char byte = 'x';
    ASSERT_EQ(write(sp.writeEnd, &byte, 1), 1);

    EXPECT_EQ(reactor.poll(100), 0);
    EXPECT_FALSE(called);
}

TEST(EpollReactor, EintrIsRetriedTransparently) {
    // Install a no-op handler for a signal that does NOT set SA_RESTART, so
    // epoll_wait is interrupted mid-syscall with EINTR. poll() must retry
    // rather than crash or propagate the interruption to the caller.
    struct sigaction sa{};
    sa.sa_handler = [](int) {};
    sa.sa_flags = 0; // deliberately no SA_RESTART
    sigemptyset(&sa.sa_mask);
    ASSERT_EQ(sigaction(SIGUSR1, &sa, nullptr), 0);

    asyncnet::EpollReactor reactor;
    SocketPair sp;

    std::atomic<bool> fired{false};
    reactor.registerFd(sp.readEnd, asyncnet::IOEvent::Readable,
                        [&](int, asyncnet::IOEvent) { fired = true; });

    const pthread_t mainThread = pthread_self();

    std::thread interrupter([&]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        pthread_kill(mainThread, SIGUSR1);

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        const char byte = 'x';
        const ssize_t written = write(sp.writeEnd, &byte, 1);
        (void)written;
    });

    // Blocks well past the SIGUSR1 delivery; if EINTR were not retried,
    // this would return early (n == -1, mapped to an exception) instead of
    // waiting for the real write.
    const int n = reactor.poll(5000);

    interrupter.join();

    EXPECT_EQ(n, 1);
    EXPECT_TRUE(fired.load());

    std::signal(SIGUSR1, SIG_DFL);
}
