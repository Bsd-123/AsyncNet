#include "asyncnet/acceptor.hpp"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstdint>
#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include "asyncnet/epoll_reactor.hpp"
#include "asyncnet/event_loop.hpp"

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

// Counts how many times poll() is called, so tests can assert a burst of
// events was handled within a single reactor wakeup rather than trickling
// in across several run() loop iterations.
class CountingReactor : public asyncnet::IOReactor {
public:
    explicit CountingReactor(std::unique_ptr<asyncnet::IOReactor> inner)
        : inner_(std::move(inner)) {}

    void registerFd(int fd, asyncnet::IOEvent events,
                     asyncnet::EventHandler handler) override {
        inner_->registerFd(fd, events, std::move(handler));
    }
    void modifyFd(int fd, asyncnet::IOEvent events) override {
        inner_->modifyFd(fd, events);
    }
    void removeFd(int fd) override { inner_->removeFd(fd); }
    int poll(int timeoutMs) override {
        ++pollCalls;
        return inner_->poll(timeoutMs);
    }

    int pollCalls = 0;

private:
    std::unique_ptr<asyncnet::IOReactor> inner_;
};

// The fd number the next open()/socket()/accept4() call would receive (the
// lowest currently-unused descriptor). fd allocation always picks the
// lowest free number, and fd numbering can have gaps, so this -- not a
// count of open fds -- is what determines when RLIMIT_NOFILE actually
// bites: opened and immediately closed, freeing the slot right back up.
int probeNextFdNumber() {
    const int fd = open("/dev/null", O_RDONLY);
    if (fd >= 0) close(fd);
    return fd;
}

// Temporarily tightens RLIMIT_NOFILE so a subsequent fd allocation (e.g.
// accept4()) deterministically fails with EMFILE, then restores it.
class TightFdLimitGuard {
public:
    explicit TightFdLimitGuard(rlim_t newSoftLimit) {
        getrlimit(RLIMIT_NOFILE, &original_);
        rlimit updated{newSoftLimit, original_.rlim_max};
        setrlimit(RLIMIT_NOFILE, &updated);
    }
    ~TightFdLimitGuard() { setrlimit(RLIMIT_NOFILE, &original_); }

    TightFdLimitGuard(const TightFdLimitGuard&) = delete;
    TightFdLimitGuard& operator=(const TightFdLimitGuard&) = delete;

private:
    rlimit original_{};
};

} // namespace

TEST(Acceptor, AcceptsSingleConnection) {
    asyncnet::EventLoop loop;

    int acceptedFd = -1;
    asyncnet::Acceptor acceptor(loop, 0, [&](int fd) {
        acceptedFd = fd;
        loop.stop();
    });

    const int clientFd = connectLoopbackBlocking(acceptor.boundPort());
    ASSERT_GE(clientFd, 0);

    loop.run();

    ASSERT_GE(acceptedFd, 0);

    const char byte = 'z';
    ASSERT_EQ(write(clientFd, &byte, 1), 1);
    char received = 0;
    EXPECT_EQ(read(acceptedFd, &received, 1), 1);
    EXPECT_EQ(received, byte);

    close(acceptedFd);
    close(clientFd);
}

TEST(Acceptor, AcceptsBurstOfConnectionsFromSingleWakeup) {
    auto countingReactor =
        std::make_unique<CountingReactor>(std::make_unique<asyncnet::EpollReactor>());
    CountingReactor* counter = countingReactor.get();
    asyncnet::EventLoop loop(std::move(countingReactor));

    constexpr int kClientCount = 5;
    std::vector<int> acceptedFds;
    asyncnet::Acceptor acceptor(loop, 0, [&](int fd) {
        acceptedFds.push_back(fd);
        if (static_cast<int>(acceptedFds.size()) == kClientCount) {
            loop.stop();
        }
    });

    const std::uint16_t port = acceptor.boundPort();

    // All clients connect (and land in the kernel accept backlog) before
    // run() ever calls poll(), so a single wakeup must drain the burst.
    std::vector<int> clientFds;
    for (int i = 0; i < kClientCount; ++i) {
        const int cfd = connectLoopbackBlocking(port);
        ASSERT_GE(cfd, 0);
        clientFds.push_back(cfd);
    }

    loop.run();

    EXPECT_EQ(static_cast<int>(acceptedFds.size()), kClientCount);
    EXPECT_EQ(counter->pollCalls, 1);

    for (int fd : acceptedFds) close(fd);
    for (int fd : clientFds) close(fd);
}

TEST(Acceptor, EmfileIsHandledWithoutCrashOrBusyLoop) {
    asyncnet::EventLoop loop;

    int acceptedCount = 0;
    asyncnet::Acceptor acceptor(loop, 0, [&](int fd) {
        ++acceptedCount;
        close(fd);
    });

    const int clientFd = connectLoopbackBlocking(acceptor.boundPort());
    ASSERT_GE(clientFd, 0);

    {
        // Tighten the fd limit to exactly the next fd number that would be
        // allocated, so the very next fd allocation attempt (accept4()
        // inside the acceptor) deterministically fails with EMFILE.
        TightFdLimitGuard guard(static_cast<rlim_t>(probeNextFdNumber()));

        // A bounded, single poll() call: must not throw, crash, or busy
        // loop -- and per the documented policy, must not accept anything
        // this wakeup either.
        EXPECT_NO_THROW(loop.reactor().poll(1000));
        EXPECT_EQ(acceptedCount, 0);
    }

    // Limit restored: the still-pending connection can now be accepted
    // normally, proving the reactor/acceptor kept working afterwards.
    EXPECT_EQ(loop.reactor().poll(1000), 1);
    EXPECT_EQ(acceptedCount, 1);

    close(clientFd);
}
